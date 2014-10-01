/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.task;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.app.AppFunction;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.TupleflowTuple;

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
    @InputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowTuple")
    @OutputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowTuple")
    public static class WriterContentToFile extends TaskProcessor {

        @Override
        public void close() throws IOException {
            processor.close();
        }

        @Override
        public void process(TupleflowTuple object) throws IOException {
            processor.process(object);
        }

    }
}
