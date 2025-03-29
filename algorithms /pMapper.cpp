//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

#include "Scheduler.hpp"
#include <climits>
#include <algorithm>

static bool migrating = false;
static unsigned active_machines = 16;

VMType_t Scheduler::GetDefaultVMForCPU(CPUType_t cpu_type) {
    switch (cpu_type) {
        case X86:
            return LINUX;
        case POWER:
            return AIX;
        case ARM:
            return WIN;
        default:
            SimOutput("Scheduler::GetDefaultVMForCPU(): Unknown CPU type " + to_string(cpu_type), 1);
            return VMType_t(-1); // Fallback VM type
    }
 }

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    unsigned total_machines = Machine_GetTotal();
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);



    for (unsigned i = 0; i < total_machines; i++) {
        machines.push_back(MachineId_t(i));
        machineQueue.push(MachineId_t(i)); 
        MachineInfo_t machine_info = Machine_GetInfo(i); 
        VMId_t vm = VM_Create(GetDefaultVMForCPU(machine_info.cpu), machine_info.cpu);
        vms.push_back(vm);

        //used AI here to know how to push elements onto a hashmap in C++ 
        machinesToVMs[machines[i]] = {};
        machinesToVMs[machines[i]].push_back(vms[i]);
        VM_Attach(vm, i);
 
    }

    bool dynamic = false;
    if(dynamic)
        for(unsigned i = 0; i<4 ; i++)
            for(unsigned j = 0; j < 8; j++)
                Machine_SetCorePerformance(MachineId_t(0), j, P3);

    SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " + to_string(vms[1]), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
    auto it = std::find(pending_vms.begin(), pending_vms.end(), vm_id);

    if (it != pending_vms.end()) {
        pending_vms.erase(it);
    }

}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) { 
   vector<MachineId_t> prevMachines;
   TaskInfo_t task_info = GetTaskInfo(task_id); 

   //needed AI to see how to use priority queue functionality in C++ 
   MachineId_t top = machineQueue.top();
   prevMachines.push_back(top);
   machineQueue.pop(); 

   while(!machineQueue.empty()) {
      MachineInfo_t m_info = Machine_GetInfo(top); 
      unsigned available_memory = m_info.memory_size - m_info.memory_used;
      if (m_info.s_state == S0 && m_info.cpu == task_info.required_cpu && (available_memory >= task_info.required_memory + VM_MEMORY_OVERHEAD)) {
         //find a available VM or create one for this machine 
         for(VMId_t vm : vms) {
            VMInfo_t vm_info = VM_GetInfo(vm); 
            auto migrating = std::find(pending_vms.begin(), pending_vms.end(), vm);
            if(vm_info.machine_id == top && vm_info.vm_type == task_info.required_vm && migrating != pending_vms.end()) {
               VM_AddTask(vm, task_id, task_info.priority);
               for(MachineId_t removed_machines: prevMachines) {
                  machineQueue.push(removed_machines); 
               }
               return; 
            }
         }

         VMId_t new_vm = VM_Create(task_info.required_vm, task_info.required_cpu);
         machinesToVMs[top].push_back(new_vm); 
         VM_Attach(new_vm, top);
         VM_AddTask(new_vm, task_id, task_info.priority);

         vms.push_back(new_vm);

         for(MachineId_t removed_machines: prevMachines) {
            machineQueue.push(removed_machines); 
         }
         return; 
      }

      top = machineQueue.top();
      prevMachines.push_back(top);
      machineQueue.pop(); 
   }


   //if we were not able to find a machine, try turning one on 
   for(unsigned i = 0; i < Machine_GetTotal(); i++) {
      MachineInfo_t m_info = Machine_GetInfo(MachineId_t(i)); 
      unsigned available_memory = m_info.memory_size - m_info.memory_used;
      if (m_info.s_state == S5 && m_info.cpu == task_info.required_cpu && (available_memory >= task_info.required_memory + VM_MEMORY_OVERHEAD)) {
         Machine_SetState(MachineId_t(i), S0);
         VMId_t new_vm = VM_Create(task_info.required_vm, task_info.required_cpu);
         machinesToVMs[MachineId_t(i)].push_back(new_vm); 
         VM_Attach(new_vm, MachineId_t(i));
         VM_AddTask(new_vm, task_id, task_info.priority);

         vms.push_back(new_vm);
      }
   }


   //push everything back on our priority queue
   for(MachineId_t removed_machines : prevMachines) {
      machineQueue.push(removed_machines); 
   }

}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy


   std::vector<MachineId_t> split_in_half;

   while (!machineQueue.empty()) {
      split_in_half.push_back(machineQueue.top());
      machineQueue.pop();
   }

   size_t mid = split_in_half.size() / 2;
   //needed AI to see how to split a list in C++ 
   std::vector<MachineId_t> lowUtil(split_in_half.begin(), split_in_half.begin() + mid);
   std::vector<MachineId_t> highUtil(split_in_half.begin() + mid, split_in_half.end());

   TaskInfo_t task_info = GetTaskInfo(task_id); 
   for(MachineId_t curr_machine : lowUtil) {
      unsigned smallest_workload = UINT_MAX; 
      VMId_t smallest_vm_id = -1; 
      bool found_vm = false; 




      for(VMId_t vm : machinesToVMs[curr_machine]) {
         VMInfo_t vm_info = VM_GetInfo(vm); 
         auto migrating = std::find(pending_vms.begin(), pending_vms.end(), smallest_vm_id);
         if(migrating != pending_vms.end()) {
            unsigned this_workload = 0; 
            for(TaskId_t task : vm_info.active_tasks) {
               this_workload += GetTaskInfo(task).required_memory; 
               found_vm = true; 
            }

            if(this_workload > 0 && this_workload < smallest_workload) {
               smallest_workload = this_workload; 
               smallest_vm_id = vm; 
            }
         }


         if(!found_vm || smallest_vm_id <= 0) {
            break; 
         } 


         //see if we can migrate to a big machine 
         for(MachineId_t big_machine : highUtil) {
            MachineInfo_t m_info = Machine_GetInfo(big_machine);
            VMInfo_t vm_info = VM_GetInfo(smallest_vm_id); 
            unsigned available_memory = m_info.memory_size - m_info.memory_used;
            if (m_info.s_state == S0 && m_info.cpu == vm_info.cpu && (available_memory >= smallest_workload + VM_MEMORY_OVERHEAD)) {
               auto it = machinesToVMs.find(curr_machine);

                if (it != machinesToVMs.end()) {
                    std::vector<VMId_t>& vmList = it->second;
                    vmList.erase(std::remove(vmList.begin(), vmList.end(), smallest_vm_id), vmList.end());
                }           
               pending_vms.push_back(smallest_vm_id); 
               machinesToVMs[big_machine].push_back(smallest_vm_id); 
               VM_Migrate(smallest_vm_id, big_machine);
               break; 
            }
         }
      }
   }


   for(MachineId_t machine : split_in_half) {
      machineQueue.push(machine); 
   }


    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
    static unsigned counts = 0;
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
}
