import java.util.ArrayList;
import java.util.List;

public class Table {
    List<String> data;
    int columns;
    int sumFields;

    public Table(List<String> data, int columns){
        this.data = data;
        this.columns = columns;
    }

    public void DrawTable(int maxRows){
        List<Integer> width = CalculateWidth();
        for(int i = 0; i < columns; i++){
            width.set(i, width.get(i) + 6);
        }

        String separator = TableSeparators(width);
        sumFields = 0;

        System.out.println(separator);
        String header = TableFields(width);
        System.out.println(header);
        System.out.println(separator);

        while(sumFields < data.size() && (sumFields/columns-1) < maxRows){
            String row = TableFields(width);
            System.out.println(row);
        }

        System.out.println(separator);
    }

    private String TableSeparators(List<Integer> width){
        StringBuilder line = new StringBuilder("+");
        for(int i = 0; i < columns; i++){
            line.append("-".repeat(Math.max(0, width.get(i))));
            line.append("+");
        }
        return String.valueOf(line);
    }

    private String TableFields(List<Integer> width){
        StringBuilder line = new StringBuilder("|");
        for(int i = 0; i < columns; i++){
            String field = data.get(sumFields);
            int length = field.length();
            int left_spaces = (width.get(i) - length)/2;
            int right_spaces = width.get(i)-length-left_spaces;
            sumFields++;

            line.append(" ".repeat(left_spaces));
            line.append(field);
            line.append(" ".repeat(right_spaces));
            line.append("|");
        }
        return String.valueOf(line);
    }

    private List<Integer> CalculateWidth(){
        List<Integer> width = new ArrayList<Integer>();
        for(int i = 0; i < columns; i++) {
            width.add(0);
        }
        int i = 0;
        for(String d : data){
            int length = d.length();
            if(length > width.get(i%columns)){
                width.set(i%columns, length);
            }
            i++;
        }
        return width;
    }
}
