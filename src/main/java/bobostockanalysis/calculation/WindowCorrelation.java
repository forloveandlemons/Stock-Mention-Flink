package bobostockanalysis.calculation;

import bobostockanalysis.datatypes.StockMentionCount;
import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.flink.api.java.tuple.Tuple5;


public class WindowCorrelation extends ProcessAllWindowFunction<StockMentionCount, Tuple5<String, String, String, Long, Double>, TimeWindow> {


    @Override
    public void process(ProcessAllWindowFunction<StockMentionCount, Tuple5<String, String, String, Long, Double>, TimeWindow>.Context context, Iterable<StockMentionCount> input, Collector<Tuple5<String, String, String, Long, Double>> out) {
        HashSet<String> symbols = new HashSet<>();
        HashMap<String, ArrayList<Integer>> m = new HashMap<>();

        for (StockMentionCount smc: input) {
            symbols.add(smc.stock);
            m.putIfAbsent(smc.stock, new ArrayList<>());
            m.get(smc.stock).add(smc.count);
        }

        DateFormat simple = new SimpleDateFormat("yyyy-MM-dd");
        Long miliSec = context.window().getEnd();
        String ds = simple.format(new Date(miliSec));
        for (String s1: symbols) {
            for (String s2: symbols) {
                if (s1.compareTo(s2) < 0) {
                    out.collect(new Tuple5<>(ds, s1, s2, miliSec, getCorrelation(m.get(s1), m.get(s2))));
                }
            }
        }
    }

    private Double getCorrelation(ArrayList<Integer> xArray, ArrayList<Integer> yArray) {
        PearsonsCorrelation pc = new PearsonsCorrelation();
        double[] xDoubleArray = toDoubleArray(xArray);
        double[] yDoubleArray = toDoubleArray(yArray);
        try {
            return pc.correlation(xDoubleArray, yDoubleArray);
        } catch (DimensionMismatchException e) {
            Integer l1 = xDoubleArray.length;
            Integer l2 = yDoubleArray.length;
            if (l1 > l2) {
                return pc.correlation(Arrays.copyOfRange(xDoubleArray, 0, Math.min(l1, l2)), yDoubleArray);
            } else {
                return pc.correlation(xDoubleArray, Arrays.copyOfRange(yDoubleArray, 0, Math.min(l1, l2)));
            }
        } catch (MathIllegalArgumentException e) {
            return 0.;
        }
    }

    private double[] toDoubleArray(ArrayList<Integer> integersArray) {
        double[] res = new double[integersArray.size()];
        int i = 0;
        for (Integer val : integersArray) {
            res[i++] = val;
        }
        return res;
    }
}