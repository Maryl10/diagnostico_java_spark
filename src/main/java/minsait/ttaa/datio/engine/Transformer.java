package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

 
    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df = cleanData(df);
        df = addPlayerCat(df);
        df = addPotencialVsOverall(df);
        df = filterData(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        write(df);
    }

	/**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        return spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
    }
    
    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column nationality != null && column teamPosition != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
    	return df.filter(nationality.column().isNotNull()
    			.and(teamPosition.column().isNotNull())
    			.and(overall.column().isNotNull()));
    }

    /**
     * @param df
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 3 best players
     * cat B for if is in 5 best players
     * cat C for if is in 10 best players
     * cat D for the rest
     */
    
    private Dataset<Row> addPlayerCat(Dataset<Row> df) {
    	WindowSpec w = Window
    			.partitionBy(nationality.column(),teamPosition.column())
    			.orderBy(overall.column());
    	
    	Column rank = rank().over(w);
    	
    	Column rule = when(rank.$less(3), A)
    			.when(rank.$less(5), B)
    			.when(rank.$less(10), C)
    			.otherwise(D);
    	
    	df = df.withColumn(playerCat.getName(), rule);
    	
		return df;
	}
    
    private Dataset<Row> addPotencialVsOverall(Dataset<Row> df) {
		
    	Colum rule = col(potential.getName()).divide(col(overall.getName()));
    	
		return df.withColum(potencialVsOverall.getName(), rule);
	}

    private Dataset<Row> filterData(Dataset<Row> df) {
		
    	df = df.filter(playerCat.column(.isNotNull()
    			.and(playerCat.column().isin(A,B))
    			.and(when(playerCat.colum().equalTo(C),col(potencialVsOverall.getName()).$greater(1.15))
    				.when(playerCat.colum().equalTo(C),col(potencialVsOverall.getName()).$greater(1.25))
    					));
    		
		return df;
	}
    
	private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
        		shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                playerCat.column(),
                potentialVsOverall.column()
        );
    }





}
