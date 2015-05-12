
public class MyAutoScaler extends AutoScaler{

	public static void main(String[] args) {
		MyAutoScaler autoScaler = new MyAutoScaler();
		autoScaler.setconfigPath(args[0]);
		autoScaler.run();
	}

	@Override
	public void scale(WorkersInfo info) {
		System.out.println("Scale: average queue size = " + info.getAverageQueueSize());
		if (info.getAverageQueueSize() > 36) {
			addWorker(1, 1000 * 60 * 1);
		} else if (info.workers.size() == 1) {
			return;
		}
		else if ((float)info.getAverageQueueSize() * info.workers.size() / (info.workers.size() - 1) < 36) {
			removeWorker(1,  1000 * 10);
		}
	}
}
