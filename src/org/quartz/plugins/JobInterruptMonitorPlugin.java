package org.quartz.plugins;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.listeners.TriggerListenerSupport;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerPlugin;

public class JobInterruptMonitorPlugin extends TriggerListenerSupport implements SchedulerPlugin{

    private static final String JOB_INTERRUPT_MONITER_KEY = "JOB_INTERRUPT_MONITER_KEY";
    
    //TODO Read it from Quartz Configuration
    private static final long MAX_DELAY = 300000;
    
	private ScheduledExecutorService executor;
	
	private ScheduledFuture future;
	
	private Scheduler scheduler;

    public JobInterruptMonitorPlugin(){
    }

 


    @Override
    public void start() {
    }

    @Override

    public void shutdown() {
      this.executor.shutdown();     
    }

 

    public ScheduledFuture scheduleJobInterruptMonitor(JobKey jobkey, long delay) {
      return this.executor.schedule(new InterruptMonitor(jobkey,scheduler), delay, TimeUnit.MILLISECONDS);
    }

   //Trigger Listener Methods
    public String getName(){
    	return null;
    }


     public void triggerFired(Trigger trigger, JobExecutionContext context){
          //Call the scheduleJobInterruptMonitor and capture the ScheduledFuture in context
    	 try {
    		 //Schedule Monitor only if the job wants AutoInterruptable functionality
    		 if (context.getJobDetail().getJobDataMap().getBoolean("AutoInterruptable")){
				JobInterruptMonitorPlugin monitorPlugin = (JobInterruptMonitorPlugin) context.getScheduler().getContext().get(JOB_INTERRUPT_MONITER_KEY);
				//Get the MaxRuntime from Job Data if NOT available use MAX_DELAY
				long jobDataDelay = context.getJobDetail().getJobDataMap().getLong("MAX_RUN_TIME");
				if (jobDataDelay == 0){
					jobDataDelay = MAX_DELAY;
				}
				future = monitorPlugin.scheduleJobInterruptMonitor(context.getJobDetail().getKey(),jobDataDelay);
    		 }
		} catch (SchedulerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     }

     public void triggerComplete(Trigger trigger,JobExecutionContext context,CompletedExecutionInstruction triggerInstructionCode) {
         //cancel the Future if job is complete
    	 if (future != null){
    		 future.cancel(true);
    	 }
     }


	@Override
	public void initialize(String name, Scheduler scheduler, ClassLoadHelper helper) throws SchedulerException {

	     this.executor = Executors.newScheduledThreadPool(1);
	     scheduler.getContext().put(JOB_INTERRUPT_MONITER_KEY, this);
	     this.scheduler = scheduler;
	     //Set the trigger Listener as this class to the ListenerManager here
	     this.scheduler.getListenerManager().addTriggerListener(this);
		
	}


    static class InterruptMonitor implements Runnable {

      private final JobKey jobKey;
      private final Scheduler scheduler;

      InterruptMonitor(JobKey jobKey,Scheduler scheduler) {
    	  this.jobKey = jobKey;
    	  this.scheduler = scheduler;
      }

      @Override
      public void run() {
          try {

              //Interrupt the job here - using Scheduler API that gets propagated to Job's interrupt
               scheduler.interrupt(jobKey);
          } catch (SchedulerException x) {
          }
      }
    }
}