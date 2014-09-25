/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.task;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.app.AppFunction;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.lemurproject.galago.tupleflow.types.TupleflowTask;

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

        job.connect("split", "process", ConnectionAssignmentType.Each);

        return job;
    }

    private Stage getSplitStage(Parameters parameter) {
        Stage stage = new Stage("split");

        stage.addOutput("tasks", new TupleflowTask.ArgumentsOrder());

        List<String> inputFiles = parameter.getAsList("input");

        Parameters p = new Parameters();
        p.set("input", new ArrayList());
        for (String input : inputFiles) {
            p.getList("input").add(new File(input).getAbsolutePath());
        }

        stage.add(new Step(FileSource.class, p));
        stage.add(Utility.getSorter(new FileName.FilenameOrder()));
        stage.add(new Step(TaskArgFileParser.class));
        stage.add(Utility.getSorter(new TupleflowTask.ArgumentsOrder()));
        stage.add(new OutputStep("tasks"));

        return stage;
    }

    private Stage getProcessStage(Parameters parameters) {
        Stage stage = new Stage("process");

        stage.addInput("tasks", new TupleflowTask.ArgumentsOrder());

        stage.add(new InputStep("tasks"));
        stage.add(new Step(getProcessClass(), parameters));
        return stage;
    }

    /**
     * Subclass use this function to specify the process class that takes in a
     * task (its argument), and process.
     *
     * @return
     */
    protected abstract Class getProcessClass();

}
