package com.yhd.storm.scheduler;

import clojure.lang.PersistentArrayMap;
import org.apache.storm.scheduler.*;

import java.util.*;

/**

 */
public class ZoneScheduler implements IScheduler{

    @Override
    public void prepare(Map conf) {

    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        System.out.println("ZoneScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;
        TopologyDetails topology;

        //stick components to zone, 1 true, else false
        String stickZone;
        Map map;
        Iterator<String> iterator = null;

        //topologies scheduled by even
        Map<String, TopologyDetails> topologiesEven = new HashMap<>();

        for (Map.Entry<String, TopologyDetails> entry : topologies.entrySet()) {
            String tid = entry.getKey();
            TopologyDetails td = entry.getValue();

            map = td.getConf();
            stickZone = (String)map.get("stickzone");

            if(stickZone != null && stickZone.equals("1")){
                System.out.println("topology named " + td.getName()+ " will be scheduled by ZoneScheduler");
                topologyAssign(cluster, td, map);
            }else {
                //only topologies which stickzone is not 1 will be scheduled by even
                //topologies whose stickzone is 1 will never fall back to even
                topologiesEven.put(tid,td);
            }
        }

        //others scheduled by even
        new EvenScheduler().schedule(new Topologies(topologiesEven), cluster);
    }


    /**
     * @param map conf for scheduling
     */
    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map map){
        Set<String> keys;
        PersistentArrayMap stickzoneMap;
        Iterator<String> iterator;

        iterator = null;
        // make sure the special topology is submitted,
        if (topology != null) {

            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
                System.out.println("Zone sticking topology "+topology.getName()+" does not need scheduling.");
            } else {
                System.out.println("Zone sticking topology "+topology.getName()+" needs scheduling.");

                stickzoneMap = (PersistentArrayMap)map.get("stickzone_map");
                if(stickzoneMap != null){
                    System.out.println("stickzone map size is " + stickzoneMap.size());
                    keys = stickzoneMap.keySet();
                    iterator = keys.iterator();

                    System.out.println("keys size is " + keys.size());
                }

                if(stickzoneMap == null || stickzoneMap.size() == 0){
                    System.out.println("stickzone map is null or empty.");
                }

                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

                System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    System.out.println("current assignments: {}");
                }

                String componentName;
                String zoneName;
                if(stickzoneMap != null && iterator != null){
                    while (iterator.hasNext()){
                        componentName = iterator.next();
                        zoneName = (String)stickzoneMap.get(componentName);

                        System.out.println("scheduling component to zone:" + componentName + "->" + zoneName);
                        componentAssign(cluster, topology, componentToExecutors, componentName, zoneName);
                    }
                }
            }
        }
    }

    /**
     */
    private void componentAssign(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> totalExecutors, String componentName, String supervisorName){
        if (!totalExecutors.containsKey(componentName)) {
            System.out.println("Our special-spout "+topology.getName()+" does not need scheduling.");
        } else {
            System.out.println("Our special-spout "+topology.getName()+" needs scheduling.");
            List<ExecutorDetails> executors = totalExecutors.get(componentName);

            // find out the our "special-supervisor" from the supervisor metadata
            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
            SupervisorDetails specialSupervisor = null;
            for (SupervisorDetails supervisor : supervisors) {
                Map meta = (Map) supervisor.getSchedulerMeta();

                if(meta != null && meta.get("zone") != null){
                    System.out.println("supervisor zone:" + meta.get("zone"));

                    if (meta.get("zone").equals(supervisorName)) {


                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(supervisor);

                        //check if the supervisor has free slots
                        if (availableSlots.isEmpty()) {
                            //empty
                        }else{
                            System.out.println("Supervisor finded");
                            specialSupervisor = supervisor;
                            break;
                        }
                    }
                }else {
                    System.out.println("Supervisor meta null");
                }

            }

            // found the special supervisor
            if (specialSupervisor != null) {
                System.out.println("Found the special-supervisor");


                // get free slots
                availableSlots = cluster.getAvailableSlots(specialSupervisor);

                // TODO worker num, instead of only using one slot
                cluster.assign(availableSlots.get(0), topology.getId(), executors);
                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
            } else {
                System.out.println("There is no supervisor find!!!");
            }
        }
    }
}
