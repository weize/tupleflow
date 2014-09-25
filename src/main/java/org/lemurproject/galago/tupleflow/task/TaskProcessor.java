/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.task;

import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.TupleflowTask;

/**
 *
 * @author wkong
 */
@Verified
@InputClass(className = "org.lemurproject.galago.tupleflow.types.TupleflowTask")
public abstract class TaskProcessor implements Processor<TupleflowTask> {
}
