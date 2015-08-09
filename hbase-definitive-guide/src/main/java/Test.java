import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class Test {

	public static void main(String[] args) {
		List<String> keys = new ArrayList<String>();
		keys.add(Arrays.toString("row-001".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-002".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-003".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-004".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-005".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-006".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-007".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-008".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-009".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-010".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		keys.add(Arrays.toString("row-011".getBytes()).replace(",", "").replace("[", "").replace("]", "").replace(" ", ""));
		
		Collections.sort(keys);
		
		System.out.println(keys);
	}

}
