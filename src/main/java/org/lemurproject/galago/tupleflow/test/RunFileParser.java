/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.lemurproject.galago.tupleflow.types.TupleflowString;

/**
 *
 * @author wkong
 */
@Verified
@InputClass(className = "org.lemurproject.galago.tupleflow.types.FileName")
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowString")
public class RunFileParser extends StandardStep<FileName, TupleflowString> {

    @Override
    public void process(FileName fileName) throws IOException {
        
        FileInputStream stream = new FileInputStream(fileName.filename);
        BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream));
        String line;
        while ((line = reader.readLine()) != null) {
            processor.process(new TupleflowString(line));

        }
        reader.close();
    }
}
