/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.lemurproject.galago.tupleflow.test;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.TupleflowString;

/**
 *
 * @author wkong
 */
@Verified
@InputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowString")
public class WriterContentToFile implements Processor<TupleflowString>{

    @Override
    public void process(TupleflowString run) throws IOException {
        String [] elems = run.value.split("\t");
        
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(elems[0])));
        writer.write(elems[1]);
        writer.newLine();
        writer.close();
    }

    @Override
    public void close() throws IOException {
    }
    
}
