package ru.sut.fuzzybalancer;

import net.sourceforge.jFuzzyLogic.ruleAccumulationMethod.RuleAccumulationMethod;

public class RuleAccumulationMethodOWA extends RuleAccumulationMethod {

	double beta = 0.8;

	public double getBeta() {
		return beta;
	}

	public void setBeta(double beta) {
		this.beta = beta;
	}

	public RuleAccumulationMethodOWA() {
		super();
		setName("owa");
	}

	@Override
	public double aggregate(double defuzzifierValue, double valueToAggregate) {

		return beta * Math.min(defuzzifierValue, valueToAggregate) + (1 - beta)
				* 0.5 * (defuzzifierValue + valueToAggregate);
	}

	public String toStringFcl() {
		return "ACCU : OWA;";
	}

}
