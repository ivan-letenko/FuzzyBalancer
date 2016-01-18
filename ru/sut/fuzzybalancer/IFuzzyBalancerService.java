package ru.sut.fuzzybalancer;

import java.util.HashMap;

import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.routing.Link;

public interface IFuzzyBalancerService extends IFloodlightService {

	public void addListener(IFuzzyBalancerListener listener);

	public HashMap<Link, Integer> getLinkCost();

}
