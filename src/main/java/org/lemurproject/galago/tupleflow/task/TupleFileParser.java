/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.lemurproject.galago.tupleflow.task;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.lemurproject.galago.tupleflow.types.TupleflowTuple;

/**
 *
 * @author wkong
 */
@Verified
@InputClass(className = "org.lemurproject.galago.tupleflow.types.FileName")
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowTuple")
public class TupleFileParser extends StandardStep<FileName, TupleflowTuple> {
    @Override
    public void process(FileName fileName) throws IOException {    
        FileInputStream stream = new FileInputStream(fileName.filename);
        BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream));
        String line;
        while ((line = reader.readLine()) != null) {
            processor.process(new TupleflowTuple(line));

        }
        reader.close();
    }
    
}
