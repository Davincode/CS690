package com.snaplogic.cc.yarn;

import com.snaplogic.cc.PipelineGraph.SerializeGraph;
import com.snaplogic.cc.yarn.visitor.SparkVisitor;
import com.snaplogic.jpipe.core.graph.PipelineNode;

/**
 * Created by luchao on 4/27/15.
 */
public class SparkGraphManager {
    public void getFlowDefinitionFor(String ruuid,
                                        PipelineNode pipelineNode) {
        SparkVisitor sparkVisitor = new SparkVisitor();
        createSparkPipes(ruuid, pipelineNode, sparkVisitor);
        //reportErrorsFrom(spark);

        SerializeGraph serializeGraph = new SerializeGraph();
        serializeGraph.serial(sparkVisitor.getSparkPipe());
    }

    protected void createSparkPipes(String ruuid, PipelineNode pipelineNode,
                                    SparkVisitor sparkVisitor) {
        //LOG.debug(GENERATING_CASCADING_PIPES, ruuid);
        pipelineNode.perform(sparkVisitor);
    }
}
