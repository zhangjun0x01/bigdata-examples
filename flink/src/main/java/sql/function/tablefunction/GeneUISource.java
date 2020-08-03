package sql.function.tablefunction;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class GeneUISource implements SourceFunction<List> {

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private volatile boolean isRunning = true;
	private static final long serialVersionUID = 1L;
	long count = 0;
	private Date date;
	private AtomicLong al = new AtomicLong(0L);

	String province[] = new String[] { "shanghai", "yunnan", "内蒙", "北京", "吉林", "四川", "国外", "天津", "宁夏", "安徽", "山东", "山西", "广东",
			"广西", "江苏", "江西", "河北", "河南", "浙江", "海南", "湖北", "湖南", "甘肃", "福建", "贵州", "辽宁", "重庆", "陕西", "香港", "黑龙江" };

	@Override
	public void run(SourceContext<List> ctx) throws Exception{
		while (isRunning) {
			Thread.sleep(100);
			// 省市、id、datestamp、date、计数,
			List list = new ArrayList();
			date = new Date();
			StringBuffer ss = new StringBuffer();
			String pro = province[(int) (Math.random() * 29)];
			list.add(pro);
			int id = (int) (Math.random() * 5);
			list.add(id);
			list.add(date.getTime());
			list.add(sdf.format(date));
			list.add(al.incrementAndGet());
			ctx.collect(list);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

}