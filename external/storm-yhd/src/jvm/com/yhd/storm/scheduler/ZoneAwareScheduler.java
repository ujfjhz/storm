package com.yhd.storm.scheduler;

import clojure.lang.PersistentArrayMap;
import org.apache.storm.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 */
public class ZoneAwareScheduler implements IScheduler{

    private static final Logger LOG = LoggerFactory.getLogger(TopologyDetails.class);

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("ZoneAwareScheduler: begin scheduling for stickzone topologies...");
        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;

        //topologies scheduled by even
        Map<String, TopologyDetails> topologiesEven = new HashMap<>();

        topologyDetailes = topologies.getTopologies();
        for(TopologyDetails td: topologyDetailes){
            String tid = td.getId();
            Map map = td.getConf();

            //stick components to zone, 1 true, else false
            String stickZone = (String)map.get("stickzone");

            if(stickZone != null && stickZone.equals("1")){
                LOG.info("topology " + td.getName()+ " will be scheduled by ZoneAwareScheduler");
                topologyAssign(cluster, td, map);
            }else {
                //only topologies which stickzone is not 1 will be scheduled by even
                //topologies whose stickzone is 1 will never fall back to even
                topologiesEven.put(tid,td);
            }
        }

        LOG.info("ZoneAwareScheduler: end scheduling for stickzone topologies.");

        //others scheduled by even
        LOG.info("EvenScheduler: begin scheduling for normal topologies...");
        new EvenScheduler().schedule(new Topologies(topologiesEven), cluster);
        LOG.info("EvenScheduler: end scheduling for normal topologies...");
    }


    /**
     * @param map conf for scheduling
     */
    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map map){

        // make sure the special topology is submitted,
        if (topology != null) {

            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
                LOG.info("Zone sticking topology "+topology.getName()+" does not need scheduling.");
            } else {
                LOG.info("Zone sticking topology "+topology.getName()+" needs scheduling.");

                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);


                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    LOG.info("current assignments of " + topology.getName()+ ": " + currentAssignment.getExecutorToSlot());
                } else {
                    LOG.info("current assignments of " + topology.getName()+ " is {}");
                }

                LOG.info("needs scheduling(component->executor): " + componentToExecutors);
                LOG.info("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));

                PersistentArrayMap stickzoneMap = (PersistentArrayMap)map.get("stickzone_map");

                for (String componentName:componentToExecutors.keySet()) {
                    String zoneName = null;

                    if(stickzoneMap!=null){
                        zoneName = (String)stickzoneMap.get(componentName);
                    }

                    if(zoneName==null){
                        LOG.info(componentName+" hasn't set sticked zone, set it to sh as default");
                        zoneName = "sh";//default sh(nanhui)
                    }

                    LOG.info("scheduling component to zone:" + componentName + "->" + zoneName);
                    componentAssign(cluster, topology, componentToExecutors, componentName, zoneName);
                }
            }
        }
    }

    /**
     */
    private void componentAssign(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> componentToExecutors, String componentName, String zoneName){
        if (!componentToExecutors.containsKey(componentName)) {
            LOG.info("Our special-spout of "+topology.getName()+" does not need scheduling.");
        } else {
            LOG.info("Our special-spout "+topology.getName()+" needs scheduling "+componentName+".");


            // find out the our "special-supervisor" from the supervisor metadata
            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
            SupervisorDetails zoneSupervisor = null;
            for (SupervisorDetails supervisor : supervisors) {
                Map meta = (Map) supervisor.getSchedulerMeta();

                if(meta != null && meta.get("zone") != null){
                    LOG.info("supervisor zone:" + meta.get("zone"));

                    if (meta.get("zone").equals(zoneName)) {

                        List<WorkerSlot> availableSlotsTmp = cluster.getAvailableSlots(supervisor);

                        //check if the supervisor has free slots
                        if (availableSlotsTmp.isEmpty()) {
                            // if no free slots ,skip this supervisor
                        }else{
                            LOG.info("Supervisor found.");
                            zoneSupervisor = supervisor;
                            break;
                        }
                    }
                }else {
                    LOG.info("Supervisor meta null");
                }

            }

            // found the zone supervisor
            if (zoneSupervisor != null) {
                LOG.info("Found the zone-supervisor for "+zoneName);

                // get free slots
                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(zoneSupervisor);

                // TODO enhancement: worker num, instead of only using one slot
                List<ExecutorDetails> executors = componentToExecutors.get(componentName);
                cluster.assign(availableSlots.get(0), topology.getId(), executors);
                LOG.info("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
            } else {
                LOG.info("There is no zone supervisor find!!!");
            }
        }
    }
}
