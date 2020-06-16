
package cep.monitor.events;

public abstract class MonitoringEvent{
	private int rackID;

	public MonitoringEvent(int rackID){
		this.rackID = rackID;
	}

	public int getRackID(){
		return rackID;
	}

	public void setRackID(int rackID){
		this.rackID = rackID;
	}

	@Override
	public boolean equals(Object obj){
		if (obj instanceof MonitoringEvent){
			MonitoringEvent monitoringEvent = (MonitoringEvent) obj;
			return monitoringEvent.canEquals(this) && rackID == monitoringEvent.rackID;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode(){
		return rackID;
	}

	public boolean canEquals(Object obj){
		return obj instanceof MonitoringEvent;
	}
}
