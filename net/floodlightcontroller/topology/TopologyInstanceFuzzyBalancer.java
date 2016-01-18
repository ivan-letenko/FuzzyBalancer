/**
 * This is an extended, modified version of the original TopologyInstance
 * file provided with Floodlight 0.90
 * 
 */

package net.floodlightcontroller.topology;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.floodlightcontroller.core.annotations.LogMessageCategory;
import net.floodlightcontroller.routing.BroadcastTree;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.routing.RouteId;
import net.floodlightcontroller.util.LRUHashMap;
import net.sourceforge.jFuzzyLogic.membership.MembershipFunctionPieceWiseLinear;
import net.sourceforge.jFuzzyLogic.membership.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.sut.fuzzybalancer.IFuzzyBalancerService;
import ru.sut.fuzzybalancer.RuleAccumulationMethodOWA;
import edu.asu.emit.qyan.alg.control.YenTopKShortestPathsAlg;
import edu.asu.emit.qyan.alg.model.Graph;
import edu.asu.emit.qyan.alg.model.Pair;
import edu.asu.emit.qyan.alg.model.Path;
import edu.asu.emit.qyan.alg.model.Vertex;
import edu.asu.emit.qyan.alg.model.abstracts.BaseVertex;

@LogMessageCategory("Network Topology")
public class TopologyInstanceFuzzyBalancer extends TopologyInstance {

    public static final short LT_SH_LINK = 1;
    public static final short LT_BD_LINK = 2;
    public static final short LT_TUNNEL  = 3; 

    public static final int MAX_LINK_WEIGHT = 10000;
    public static final int MAX_PATH_WEIGHT = Integer.MAX_VALUE - MAX_LINK_WEIGHT - 1;
    public static final int PATH_CACHE_SIZE = 1000;

    protected IFuzzyBalancerService mpbalance;
    protected Map<Pair<Long, Long>, List<Pair<Path, Boolean>>> cacheMap = null;

    protected static Logger log = LoggerFactory.getLogger(TopologyInstanceFuzzyBalancer.class);

    public TopologyInstanceFuzzyBalancer(IFuzzyBalancerService mpbalance) {
        this.switches = new HashSet<Long>();
        this.switchPorts = new HashMap<Long, Set<Short>>();
        this.switchPortLinks = new HashMap<NodePortTuple, Set<Link>>();
        this.broadcastDomainPorts = new HashSet<NodePortTuple>();
        this.tunnelPorts = new HashSet<NodePortTuple>();
        this.blockedPorts = new HashSet<NodePortTuple>();
        this.blockedLinks = new HashSet<Link>();
        this.mpbalance = mpbalance;
        cacheMap = new HashMap<Pair<Long, Long>, List<Pair<Path, Boolean>>>();
    }
    
    public TopologyInstanceFuzzyBalancer(Map<Long, Set<Short>> switchPorts,
            Map<NodePortTuple, Set<Link>> switchPortLinks, IFuzzyBalancerService mpbalance)
	{
		this.switches = new HashSet<Long>(switchPorts.keySet());
		this.switchPorts = new HashMap<Long, Set<Short>>(switchPorts);
		this.switchPortLinks = new HashMap<NodePortTuple, 
		                           Set<Link>>(switchPortLinks);
		this.broadcastDomainPorts = new HashSet<NodePortTuple>();
		this.tunnelPorts = new HashSet<NodePortTuple>();
		this.blockedPorts = new HashSet<NodePortTuple>();
		this.blockedLinks = new HashSet<Link>();
        this.mpbalance = mpbalance;
        cacheMap = new HashMap<Pair<Long, Long>, List<Pair<Path, Boolean>>>();
		clusters = new HashSet<Cluster>();
		switchClusterMap = new HashMap<Long, Cluster>();
	}
    
    public TopologyInstanceFuzzyBalancer(Map<Long, Set<Short>> switchPorts,
            Set<NodePortTuple> blockedPorts,
            Map<NodePortTuple, Set<Link>> switchPortLinks,
            Set<NodePortTuple> broadcastDomainPorts,
            Set<NodePortTuple> tunnelPorts, IFuzzyBalancerService mpbalance){


            this.switches = new HashSet<Long>(switchPorts.keySet());
            this.switchPorts = new HashMap<Long, Set<Short>>();
            this.mpbalance = mpbalance;
            for(long sw: switchPorts.keySet()) {
                this.switchPorts.put(sw, new HashSet<Short>(switchPorts.get(sw)));
            }

            this.blockedPorts = new HashSet<NodePortTuple>(blockedPorts);
            this.switchPortLinks = new HashMap<NodePortTuple, Set<Link>>();
            for(NodePortTuple npt: switchPortLinks.keySet()) {
                this.switchPortLinks.put(npt, 
                                     new HashSet<Link>(switchPortLinks.get(npt)));
            }
            this.broadcastDomainPorts = new HashSet<NodePortTuple>(broadcastDomainPorts);
            this.tunnelPorts = new HashSet<NodePortTuple>(tunnelPorts);

            blockedLinks = new HashSet<Link>();
            clusters = new HashSet<Cluster>();
            switchClusterMap = new HashMap<Long, Cluster>();
            destinationRootedTrees = new HashMap<Long, BroadcastTree>();
            clusterBroadcastTrees = new HashMap<Long, BroadcastTree>();
            clusterBroadcastNodePorts = new HashMap<Long, Set<NodePortTuple>>();
            pathcache = new LRUHashMap<RouteId, Route>(PATH_CACHE_SIZE);
            cacheMap = new HashMap<Pair<Long, Long>, List<Pair<Path, Boolean>>>();
    }
    
    
    
	@Override
	protected void calculateShortestPathTreeInClusters() {
		pathcache.clear();
        destinationRootedTrees.clear();
        
        Map<Link, Integer> linkCost = mpbalance.getLinkCost();
        
        for(Cluster c: clusters) {
            for (Long node : c.links.keySet()) {
                BroadcastTree tree = getBestPath(c, node, linkCost, true);//dijkstra(c, node, linkCost, true);
                destinationRootedTrees.put(node, tree);
            }
        }
	}
    private Graph fromClusterToGraph(Cluster c, Long root, Map<Link, Integer> linkCost){
    	Graph graph = new Graph();
    	graph.set_vertex_num(c.getNodes().size());
    	for(Long node : c.getNodes()){
    		BaseVertex vertex = new Vertex(node);
			graph._vertex_list.add(vertex);
			graph._id_vertex_index.put(vertex.get_id(), vertex);
    	}
    	for(Long nodeFrom : c.getLinks().keySet()){
    		for(Link l : c.getLinks().get(nodeFrom)){
    			long start_vertex_id = l.getSrc();
				long end_vertex_id = l.getDst();
				double weight = 0;
				if (linkCost == null || linkCost.get(l)==null) weight= 1;
	            else weight = linkCost.get(l);
				graph.add_edge(start_vertex_id, end_vertex_id, weight, l.getSrcPort(), l.getDstPort());
    		}
    	}
    	return graph;
    }
    /*
    private Path getFromCache(Long srcSwitch, Long dstSwitch){
    	List<Pair<Path, Boolean>> cachedElement = cacheMap.get(new Pair<Long, Long>(srcSwitch, dstSwitch));
    	if (cachedElement == null || cachedElement.size() <= 0) return null;
    	for(Pair<Path, Boolean> path : cachedElement){
    		if(path.second()) {
    			return path.first();
    		}
    	}
    	return null;
    }
    */
    protected BroadcastTree getBestPath(Cluster c, Long root, Map<Link, Integer> linkCost, boolean isDstRooted){
    	HashMap<Long, Link> nexthoplinks = new HashMap<Long, Link>();
        HashMap<Long, Integer> cost = new HashMap<Long, Integer>();
    	for(Long node : c.getNodes()){
    		nexthoplinks.put(node, null);
    		cost.put(node, MAX_PATH_WEIGHT);
    		cost.put(root, 0);
    		if(node.equals(root)) continue;
    		Path path; 
    			List<Path> l = calculateKShortestPath(c, root, node, linkCost);
    			path = selectBestPath(l);
    		long dstSwitch = path.get_vertices().get(path.get_vertices().size()-1).get_id();
    		long srcSwitch = path.get_vertices().get(path.get_vertices().size()-2).get_id();
    		Link lastLink = null;
    		for(Link link : c.getLinks().get(dstSwitch)){
    			if(link.getDst() == srcSwitch){
    				lastLink = link;
    			}
    		}
    		cost.put(node, (int)path.get_weight());
    		nexthoplinks.put(node, lastLink);
    	}
        BroadcastTree ret = new BroadcastTree(nexthoplinks, cost);
        log.info(ret.toString());
        return ret;
    }
   
    protected Path selectBestPath(List<Path> pathList){
    	Path bestPath = pathList.get(0);
    	int minH = bestPath.get_vertices().size();
    	int maxH = minH;
    	double maxWeight = 0;
    	for (Path tmpPath : pathList ){
			if (tmpPath.get_weight() > maxWeight)
				maxWeight = tmpPath.get_weight();
			if (tmpPath.get_vertices().size() > maxH)
				maxH =  tmpPath.get_vertices().size();
			if (tmpPath.get_vertices().size() < minH)
				minH =  tmpPath.get_vertices().size();
    	}
    	Value[] xVal = { new Value(minH), new Value(maxH) };
		Value[] yVal = { new Value(0.75), new Value(1) };
		MembershipFunctionPieceWiseLinear bandwidthFunction = new MembershipFunctionPieceWiseLinear(
				xVal, yVal);
		RuleAccumulationMethodOWA owa = new RuleAccumulationMethodOWA();		
		double bestCost = 1;
    	for (Path tPath : pathList ){
    	 	double pathLengthCost = bandwidthFunction.membership(tPath.get_vertices().size());
    	 	double pathWeightCost = tPath.get_weight() / maxWeight;
    	 	double cost = owa.aggregate(pathLengthCost, pathWeightCost);
    	 	if (cost < bestCost){
    	 		bestPath = tPath;
    	 		bestCost = cost;
    	 	}    	 	
		}
		return bestPath;
		
    }
    
    protected List<Path> calculateKShortestPath(Cluster c, Long root, Long node, Map<Link, Integer> linkCost){
    	Graph graph = fromClusterToGraph(c, root, linkCost);
    	YenTopKShortestPathsAlg yenAlg = new YenTopKShortestPathsAlg(graph);
    	List<Path> pathList = yenAlg.get_shortest_paths(graph.get_vertex(root), graph.get_vertex(node), 3);
    	
    	return pathList;
    }
    
    
}