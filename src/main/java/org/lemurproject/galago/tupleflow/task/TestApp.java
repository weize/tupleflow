/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.task;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.app.AppFunction;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.TupleflowTask;

/**
 *
 * @author wkong
 */
public class TestApp extends TaskAppFunction {

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public String getHelpString() {
        return "tupleflow " + getName() + " [parameters...]\n"
                + AppFunction.getTupleFlowParameterString();
    }

    @Override
    protected Class getProcessClass() {
        return WriterContentToFile.class;
    }

    @Verified
    @InputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowTask")
    public static class WriterContentToFile extends TaskProcessor {

        @Override
        public void close() throws IOException {
        }

        @Override
        public void process(TupleflowTask task) throws IOException {
            String[] elems = task.arguments.split("\t");

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(elems[0])));
            writer.write(elems[1]);
            writer.newLine();
            writer.close();
        }

    }
}
