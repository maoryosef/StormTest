package com.my.test;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class PrimeNumberTopology
{
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new NumberSpout(), 3);
        builder.setBolt("prime", new PrimeNumberBolt(), 3)
                .shuffleGrouping("spout");

        Config conf = new Config();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
        System.out.println("done");
    }
}
