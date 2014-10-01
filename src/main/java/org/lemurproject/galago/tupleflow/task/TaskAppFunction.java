/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.task;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.app.AppFunction;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.lemurproject.galago.tupleflow.types.TupleflowTuple;

/**
 * Split the task and process. It takes in "input" file or files, splits into
 * lines, and pass each line into a processor as a split task.
 *
 * @author wkong
 */
public abstract class TaskAppFunction extends AppFunction {

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
        Job job = createJob(p);
        AppFunction.runTupleFlowJob(job, p, output);

    }

    private Job createJob(Parameters parameters) {
        Job job = new Job();

        job.add(getSplitStage(parameters));
        job.add(getProcessStage(parameters));
        job.add(getWriteStage(parameters));

        job.connect("split", "process", ConnectionAssignmentType.Each);
        job.connect("process", "write", ConnectionAssignmentType.Combined);

        return job;
    }

    private Stage getSplitStage(Parameters parameter) {
        Stage stage = new Stage("split");

        stage.addOutput("tasks", new TupleflowTuple.FieldsOrder());

        List<String> inputFiles = parameter.getAsList("input");

        Parameters p = new Parameters();
        p.set("input", new ArrayList());
        for (String input : inputFiles) {
            p.getList("input").add(new File(input).getAbsolutePath());
        }

        stage.add(new Step(FileSource.class, p));
        stage.add(Utility.getSorter(new FileName.FilenameOrder()));
        stage.add(new Step(TupleFileParser.class));
        stage.add(Utility.getSorter(new TupleflowTuple.FieldsOrder()));
        stage.add(new OutputStep("tasks"));

        return stage;
    }

    private Stage getProcessStage(Parameters parameters) {
        Stage stage = new Stage("process");

        stage.addInput("tasks", new TupleflowTuple.FieldsOrder());
        stage.addOutput("results", new TupleflowTuple.FieldsOrder());

        stage.add(new InputStep("tasks"));
        stage.add(new Step(getProcessClass(), parameters));
        stage.add(Utility.getSorter(new TupleflowTuple.FieldsOrder()));
        stage.add(new OutputStep("results"));

        return stage;
    }

    private Stage getWriteStage(Parameters parameters) {
        Stage stage = new Stage("write");

        stage.addInput("results", new TupleflowTuple.FieldsOrder());

        stage.add(new InputStep("results"));
        stage.add(new Step(TupleWriter.class, parameters));

        return stage;
    }

    @Verified
    @InputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowTuple")
    public static class TupleWriter implements Processor<TupleflowTuple> {
        
        BufferedWriter writer;
        public TupleWriter(TupleFlowParameters p) throws FileNotFoundException {
            String filename = p.getJSON().getString("output");
            FileOutputStream stream = new FileOutputStream(new File(filename));
            writer = new BufferedWriter(new OutputStreamWriter(stream));
        }

        @Override
        public void process(TupleflowTuple tuple) throws IOException {
            writer.write(tuple.fields);
            writer.write('\n');
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    /**
     * Subclass use this function to specify the process class that takes in a
     * task (its argument), and process.
     *
     * @return
     */
    protected abstract Class getProcessClass();

}
