//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//


#include "Scheduler.hpp"
#include <climits>


static bool migrating = false;
static unsigned active_machines = 16;
static unsigned round_robin_pointer = 0;


Priority_t determinePriority(SLAType_t sla) {
   switch (sla) {
       case SLA0: return HIGH_PRIORITY;
       case SLA1: return HIGH_PRIORITY;
       case SLA2: return MID_PRIORITY;
       case SLA3: return LOW_PRIORITY;
       default: return MID_PRIORITY;
   }
}


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
   unsigned total_machines = Machine_GetTotal();
   SimOutput("Scheduler::Init(): Total number of machines is " + to_string(total_machines), 3);
   SimOutput("Scheduler::Init(): Initializing scheduler", 1);


   for (unsigned i = 0; i < total_machines; i++) {
       machines.push_back(i);
       powered_on.insert(i); // Track that machine is on
       MachineInfo_t machine_info = Machine_GetInfo(i); 
       VMId_t vm = VM_Create(GetDefaultVMForCPU(machine_info.cpu), machine_info.cpu);
       VM_Attach(vm, i);


       vms.push_back(vm);
       vm_to_machine[vm] = i;
   }

    SimOutput("Scheduler::Init(): Initialized " + to_string(active_machines) + " X86 machines with VMs.", 3);

}


void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
   // Update your data structure. The VM now can receive new tasks
}


void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
   TaskInfo_t task_info = GetTaskInfo(task_id);

   for(int i = 0; i < Machine_GetTotal(); i++) {
      size_t index = (round_robin_pointer + i) % Machine_GetTotal(); //resets the index when it hits the max 
      MachineId_t machine_id = MachineId_t(index);
      MachineInfo_t machine_info = Machine_GetInfo(machine_id);

      if (machine_info.s_state != S0 || machine_info.cpu != task_info.required_cpu) continue;
      unsigned available_memory = machine_info.memory_size - machine_info.memory_used;
      if (available_memory < task_info.required_memory) continue;
      
      VMId_t foundVM;
      bool found_a_vm = false; 
      for(VMId_t vm_id : vms) {
         VMInfo_t vm_info = VM_GetInfo(vm_id); 
         if (vm_info.machine_id == machine_id && vm_info.vm_type == task_info.required_vm) {
            foundVM = vm_id; 
            found_a_vm = true;
         }
      }
      // Create a new VM
      if(!found_a_vm) {
         foundVM = VM_Create(task_info.required_vm, task_info.required_cpu);
         VM_Attach(foundVM, machine_id);
         vms.push_back(foundVM);
      }

      VM_AddTask(foundVM, task_id, task_info.priority);
      round_robin_pointer = (index + 1) % Machine_GetTotal();
      return; 
   }

   // We need to activate a machine
   for(int i = 0; i < Machine_GetTotal(); i++) {
      size_t index = (round_robin_pointer + i) % Machine_GetTotal(); //resets the index when it hits the max 
      MachineId_t machine_id = MachineId_t(index);
      MachineInfo_t machine_info = Machine_GetInfo(machine_id);

      if (machine_info.s_state != S5 || machine_info.cpu != task_info.required_cpu) continue;
      
      unsigned available_memory = machine_info.memory_size - machine_info.memory_used;
      if (available_memory < task_info.required_memory + VM_MEMORY_OVERHEAD) continue;

      Machine_SetState(machine_id, S0);
      VMId_t new_vm = VM_Create(task_info.required_vm, task_info.required_cpu);
      VM_Attach(new_vm, machine_id);
      VM_AddTask(new_vm, task_id, task_info.priority);

      vms.push_back(new_vm);
      
      round_robin_pointer = (index + 1) % Machine_GetTotal();
      return;

   }

   SimOutput("NewTask(): No placement found for task " + to_string(task_id), 1);
}


void Scheduler::PeriodicCheck(Time_t now) {
   // This method should be called from SchedulerCheck()
   // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
   // Unlike the other invocations of the scheduler, this one doesn't report any specific event
   // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
   for (MachineId_t machine : machines) {
       MachineInfo_t machine_info = Machine_GetInfo(machine);
       if (machine_info.active_tasks == 0 && machine_info.active_vms == 0 && machine_info.s_state == S0) {
           Machine_SetState(machine, S5);
       }
   }
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
   SimOutput("Total Energy: " + to_string(Machine_GetClusterEnergy()) + " KW-Hour", 1);
   SimOutput("SLA0: " + to_string(GetSLAReport(SLA0)) + "%", 1);
   SimOutput("SLA1: " + to_string(GetSLAReport(SLA1)) + "%", 1);
   SimOutput("SLA2: " + to_string(GetSLAReport(SLA2)) + "%", 1);
   SimOutput("SLA3: best-effort", 1);
}


void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
   // Do any bookkeeping necessary for the data structures
   // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
   // This is an opportunity to make any adjustments to optimize performance/energy

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
   // static unsigned counts = 0;
   // counts++;
   // if(counts == 10) {
   //     migrating = true;
   //     VM_Migrate(1, 9);
   // }
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
   SetTaskPriority(task_id, HIGH_PRIORITY);
}


void StateChangeComplete(Time_t time, MachineId_t machine_id) {
   // Called in response to an earlier request to change the state of a machine
  
}





