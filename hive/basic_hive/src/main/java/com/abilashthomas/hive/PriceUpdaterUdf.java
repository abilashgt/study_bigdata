package com.abilashthomas.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by Abilash George Thomas on 12/27/2016.
 * Email: abhilash.z.thomas.ap@nielsen.com
 */
public class PriceUpdaterUdf extends UDF {
    public double evaluate(double price, double multiplier){
        return price*multiplier;
    }
}
