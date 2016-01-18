package ru.sut.fuzzybalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscovery.LDUpdate;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;
import net.sourceforge.jFuzzyLogic.membership.MembershipFunctionPieceWiseLinear;
import net.sourceforge.jFuzzyLogic.membership.Value;

import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFStatisticsRequest;
import org.openflow.protocol.statistics.OFPortStatisticsReply;
import org.openflow.protocol.statistics.OFPortStatisticsRequest;
import org.openflow.protocol.statistics.OFStatistics;
import org.openflow.protocol.statistics.OFStatisticsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FuzzyBalancer implements IFloodlightModule, ITopologyListener,
		IFuzzyBalancerService {

	protected static Logger log = LoggerFactory.getLogger(FuzzyBalancer.class);

	public static final int UPDATE_PERIOD = 10; // seconds
	public static final int MAX_LINK_COST = 100; // MIN = 1

	protected IFloodlightProviderService floodlightProvider;
	protected ITopologyService topology;
	protected ILinkDiscoveryService linkDiscoverer;
	protected SingletonTask updateTask;
	protected IThreadPoolService threadPool;

	// Data structures for caching counters
	protected Map<Link, LinkCostInfo> linkBytes;
	// Data structures for caching algorithm results
	protected Map<Link, Integer> linkCost;

	protected ArrayList<IFuzzyBalancerListener> costAware;

	double minMembershipBW = 0.01;
	double maxMembershipBW = 1;

	protected volatile boolean shuttingDown = false;

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFuzzyBalancerService.class);
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
		m.put(IFuzzyBalancerService.class, this);
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IThreadPoolService.class);

		return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProvider = context
				.getServiceImpl(IFloodlightProviderService.class);
		topology = context.getServiceImpl(ITopologyService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		linkDiscoverer = context.getServiceImpl(ILinkDiscoveryService.class);

		linkBytes = new HashMap<Link, LinkCostInfo>();
		linkCost = new HashMap<Link, Integer>();

		costAware = new ArrayList<IFuzzyBalancerListener>();
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		if (topology != null)
			topology.addListener(this);

		ScheduledExecutorService ses = threadPool.getScheduledExecutor();
		updateTask = new SingletonTask(ses, new Runnable() {
			@Override
			public void run() {
				try {
					updateStatistics();
					updateCosts();

				} catch (Exception e) {
					log.error("Exception in Stats send timer.", e);
					updateTask.reschedule(UPDATE_PERIOD, TimeUnit.SECONDS);
				} finally {
					if (!shuttingDown) {

						updateTask.reschedule(UPDATE_PERIOD, TimeUnit.SECONDS);
					}
				}
			}
		});
		updateTask.reschedule(UPDATE_PERIOD, TimeUnit.SECONDS);

	}

	protected void updateStatistics() {

		Map<Long, Set<Link>> sLinks = linkDiscoverer.getSwitchLinks();
		Map<Long, IOFSwitch> switches = floodlightProvider.getSwitches();

		for (Map.Entry<Long, IOFSwitch> sw : switches.entrySet()) {

			List<OFStatistics> portsStat = getPortStatistics(sw.getValue());
			Set<Link> links = sLinks.get(sw.getKey());

			for (OFStatistics stat : portsStat) {
				for (Link link : links) {
					if ((sw.getKey() == (link.getSrc()) && ((OFPortStatisticsReply) stat)
							.getPortNumber() == link.getSrcPort())) {
						updateLinkInfo(link,
								((OFPortStatisticsReply) stat)
										.getTransmitBytes());
					}
				}
			}
		}
	}

	protected void updateLinkInfo(Link link, long transmitBytes) {
		if (linkBytes.containsKey(link)) {
			linkBytes.get(link).updateBytesTransferred(transmitBytes);
		} else {
			LinkCostInfo lInfo = new LinkCostInfo();
			lInfo.updateBytesTransferred(transmitBytes);
			linkBytes.put(link, lInfo);
		}
	}

	protected void updateCosts() {
		linkCost.clear();
		boolean isChanged = false;
		long maxBandwidth = 0;
		long minBandwidth = 0;
		for (Map.Entry<Link, LinkCostInfo> linkStat : linkBytes.entrySet()) {
			long delta = linkStat.getValue().getBytesDelta();
			if (maxBandwidth < delta)
				maxBandwidth = delta;
			if (minBandwidth > delta)
				minBandwidth = delta;
		}

		Value[] xVal = { new Value(minBandwidth), new Value(maxBandwidth) };
		Value[] yVal = { new Value(minMembershipBW), new Value(maxMembershipBW) };
		MembershipFunctionPieceWiseLinear bandwidthFunction = new MembershipFunctionPieceWiseLinear(
				xVal, yVal);

		for (Map.Entry<Link, LinkCostInfo> linkStat : linkBytes.entrySet()) {
			long delta = linkStat.getValue().getBytesDelta();
			int cost = (int) (bandwidthFunction.membership(delta) * MAX_LINK_COST);
			if (cost < 1)
				cost = 1;
			if (cost != linkStat.getValue().getCost()) {
				linkStat.getValue().setCost(cost);
				isChanged = true;
			}
			linkCost.put(linkStat.getKey(), cost);
			log.info(linkStat.getKey().toString() + " Cost: " + cost);
		}

		if (isChanged) {
			informListeners();
		}

		// log.info(linkStat.getKey().toString() + " Bytes: " +
		// linkStat.getValue().getBytesDelta());
	}

	protected List<OFStatistics> getPortStatistics(IOFSwitch sw) {
		Future<List<OFStatistics>> future;
		List<OFStatistics> values = null;
		OFStatisticsRequest req = new OFStatisticsRequest();
		req.setStatisticType(OFStatisticsType.PORT);
		int requestLength = req.getLengthU();
		OFPortStatisticsRequest specificReq = new OFPortStatisticsRequest();
		specificReq.setPortNumber((short) OFPort.OFPP_NONE.getValue());
		req.setStatistics(Collections.singletonList((OFStatistics) specificReq));
		requestLength += specificReq.getLength();

		req.setLengthU(requestLength);
		try {
			future = sw.getStatistics(req);
			values = future.get(10, TimeUnit.SECONDS);
		} catch (Exception e) {
			log.error("Failure retrieving statistics from switch " + sw, e);

		}
		return values;
	}

	@Override
	public void topologyChanged() {
		for (LDUpdate update : topology.getLastLinkUpdates()) {
			if (update.getOperation().equals(
					ILinkDiscovery.UpdateOperation.LINK_UPDATED)) {
				Link linkUpdate = new Link(update.getSrc(),
						update.getSrcPort(), update.getDst(),
						update.getDstPort());
				if (!linkCost.containsKey(linkUpdate)) {
					linkCost.put(linkUpdate, 1);
				}
			} else if (update.getOperation().equals(
					ILinkDiscovery.UpdateOperation.LINK_REMOVED)) {
				Link linkUpdate = new Link(update.getSrc(),
						update.getSrcPort(), update.getDst(),
						update.getDstPort());
				linkCost.remove(linkUpdate);
			}
		}
	}

	@Override
	public void addListener(IFuzzyBalancerListener listener) {
		costAware.add(listener);
	}

	public void informListeners() {

		for (int i = 0; i < costAware.size(); ++i) {
			IFuzzyBalancerListener listener = costAware.get(i);
			listener.costChanged();
		}
	}

	@Override
	public HashMap<Link, Integer> getLinkCost() {
		return (HashMap<Link, Integer>) linkCost;
	}

}
