/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.test;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.JobExecutor;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.lemurproject.galago.tupleflow.types.TupleflowString;

/**
 *
 * @author wkong
 */
public class Test {

    public static void main(String[] args) throws Exception {
        Parameters p = new Parameters();
        if (args.length == 1) {
            System.out.print("need more arguments");
            return;

        } else if (args.length > 1) {
            p = Parameters.parseArgs(Utility.subarray(args, 1));
            // don't want to wipe an existing parameter:
        }

        Job job = createJob(p);
        
        runTupleFlowJob(job, p, System.out);

    }

    public static boolean runTupleFlowJob(Job job, Parameters p, PrintStream output) throws Exception {
        if (p.isBoolean("printJob") && p.getBoolean("printJob")) {
            p.remove("printJob");
            p.set("printJob", "dot");
        }

        String printJob = p.get("printJob", "none");
        if (printJob.equals("plan")) {
            output.println(job.toString());
            return true;
        } else if (printJob.equals("dot")) {
            output.println(job.toDotString());
            return true;
        }

        int hash = (int) p.get("distrib", 0);
        if (hash > 0) {
            job.properties.put("hashCount", Integer.toString(hash));
            // System.out.println(job.properties.get("hashCount"));
        }

        ErrorStore store = new ErrorStore();
        JobExecutor.runLocally(job, store, p);
        if (store.hasStatements()) {
            output.println(store.toString());
            return false;
        }
        return true;
    }

    private static Job createJob(Parameters p) {
        Job job = new Job();
        
        job.add(getSplitStage(p));
        job.add(getProcessSrage(p));
        
        job.connect("split", "process", ConnectionAssignmentType.Each);
        
        return job;
    }

    private static Stage getSplitStage(Parameters parameters) {
        Stage stage = new Stage("split");

        stage.addOutput("runs", new TupleflowString.ValueOrder());

        List<String> inputFiles = parameters.getAsList("input");

        Parameters p = new Parameters();
        p.set("input", new ArrayList());
        for (String input : inputFiles) {
            p.getList("input").add(new File(input).getAbsolutePath());
        }

        stage.add(new Step(FileSource.class, p));
        stage.add(Utility.getSorter(new FileName.FilenameOrder()));
        stage.add(new Step(RunFileParser.class));
        stage.add(Utility.getSorter(new TupleflowString.ValueOrder()));
        stage.add(new OutputStep("runs"));

        return stage;
    }

    private static Stage getProcessSrage(Parameters p) {
        Stage stage = new Stage("process");

        stage.addInput("runs", new TupleflowString.ValueOrder());

        stage.add(new InputStep("runs"));
        stage.add(new Step(WriterContentToFile.class, p));
        return stage;
    }

}
