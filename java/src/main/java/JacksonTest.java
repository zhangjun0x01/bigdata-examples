import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhangjun 欢迎关注我的公众号[大数据技术与应用实战],获取更多精彩实战内容
 * <p>
 * 使用Jackson替代频繁爆出安全漏洞的fastjson
 * 主要记录一些Jackson的一些常用操作
 */
public class JacksonTest{

	/**
	 * 使用一个简单的json对象ObjectNode和数组对象ArrayNode，
	 * 类似fastjson中的JsonObject,Jackson无法new一个对象，
	 * 是通过ObjectMapper的工厂方法创建出来的.
	 */
	@Test
	public void testJsonObject(){
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode json = mapper.createObjectNode();
		json.put("name", "Tom");
		json.put("age", 1);
		System.out.println(json);

		ArrayNode jsonNodes = mapper.createArrayNode();
		jsonNodes.add(json);
		System.out.println(jsonNodes);
	}

	/**
	 * 序列化操作
	 *
	 * @throws JsonProcessingException
	 */
	@Test
	public void testSerialize() throws JsonProcessingException{
		User user = new User();
		user.setAge(1);
		user.setName("zhangsan");
		user.setGender(GENDER.MALE);
		user.setBirthday(new Date());
		ObjectMapper mapper = new ObjectMapper();
		String s = mapper.writeValueAsString(user);
		System.out.println(s);
	}

	/**
	 * 反序列化
	 *
	 * @throws JsonProcessingException
	 */
	@Test
	public void testDeSerialize() throws JsonProcessingException{
		String json = "{\"name\":\"zhangsan\",\"age\":10}";
		ObjectMapper mapper = new ObjectMapper();
		User user = mapper.readValue(json, User.class);
		System.out.println(user);
	}

	@Test
	public void testDeSerializeDate() throws JsonProcessingException{
		String json = "{\"name\":\"zhangsan\",\"age\":10,\"birthday\":1592800446397}";
		ObjectMapper mapper = new ObjectMapper();
		User user = mapper.readValue(json, User.class);
		System.out.println(user);

		String json1 = "{\"name\":\"zhangsan\",\"age\":10,\"birthday\":\"2020-01-01 12:13:14\"}";
		User user1 = mapper.readValue(json1, User.class);
		System.out.println(user1);

	}

	@Test
	public void testDeSerializeCustom() throws JsonProcessingException{
		String json = "{\"name\":\"zhangsan\",\"age\":10,\"birthday_custom\":\"2020-01-01 01:12:23\"}";
		ObjectMapper mapper = new ObjectMapper();
		User user = mapper.readValue(json, User.class);
		System.out.println(user);

	}

	@Test
	public void testDeSerializeWithEnum() throws JsonProcessingException{
		String json = "{\"name\":\"zhangsan\",\"age\":10,\"gender\":1}";
		ObjectMapper mapper = new ObjectMapper();
		User user = mapper.readValue(json, User.class);
		System.out.println(user);
	}

	public static class User implements java.io.Serializable{
		private String name;
		private int age;
		//用于在序列化和反序列化时，显示时间的格式
		@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
		private Date birthday;

		@JsonDeserialize(using = CustomDeserializerDate.class)
		private Date birthday_custom;
		private GENDER gender;

		public Date getBirthday(){
			return birthday;
		}

		public void setBirthday(Date birthday){
			this.birthday = birthday;
		}

		public GENDER getGender(){
			return gender;
		}

		public void setGender(GENDER gender){
			this.gender = gender;
		}

		public User(){
		}

		public String getName(){
			return name;
		}

		public void setName(String name){
			this.name = name;
		}

		public int getAge(){
			return age;
		}

		public void setAge(int age){
			this.age = age;
		}

		public Date getBirthday_custom(){
			return birthday_custom;
		}

		public void setBirthday_custom(Date birthday_custom){
			this.birthday_custom = birthday_custom;
		}

		@Override
		public String toString(){
			return "User{" +
			       "name='" + name + '\'' +
			       ", age=" + age +
			       ", birthday=" + birthday +
			       ", birthday_custom=" + birthday_custom +
			       ", gender=" + gender +
			       '}';
		}
	}

	public static class CustomDeserializerDate extends StdDeserializer<Date>{

		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		protected CustomDeserializerDate(Class<?> vc){
			super(vc);
		}

		//需要一个无参构造方法，否则会报错
		public CustomDeserializerDate(){
			this(null);
		}

		@Override
		public Date deserialize(
				JsonParser p,
				DeserializationContext ctxt) throws IOException{
			String date = p.getText();
			try {
				return sdf.parse(date);
			} catch (ParseException e){
				e.printStackTrace();
			}
			return null;
		}
	}

	public static enum GENDER{
		MALE("男", 1), FEMALE("女", 0);
		private String name;
		private int value;

		GENDER(String name, int value){
			this.name = name;
			this.value = value;
		}

		@JsonCreator
		public static GENDER getGenderById(int value){
			for (GENDER c: GENDER.values()){
				if (c.getValue() == value){
					return c;
				}
			}
			return null;
		}

		@JsonValue
		public String getName(){
			return name;
		}

		public void setName(String name){
			this.name = name;
		}

		public int getValue(){
			return value;
		}

		public void setValue(int value){
			this.value = value;
		}

	}

}
