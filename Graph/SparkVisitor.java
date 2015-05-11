package com.snaplogic.cc.yarn.visitor;

import com.snaplogic.cc.PipelineGraph.GraphPipe;
import com.snaplogic.cc.PipelineGraph.OperationPipe;
import com.snaplogic.cc.PipelineGraph.ParserPipe;
import com.snaplogic.cc.PipelineGraph.SourcePipe;
import com.snaplogic.cc.PipelineGraph.SinkPipe;
import com.snaplogic.cc.PipelineGraph.FormatterPipe;

import com.snaplogic.jpipe.core.graph.PipelineNode;
import com.snaplogic.jpipe.core.graph.SnapNode;
import com.snaplogic.jpipe.core.graph.TrackingVisitor;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

/**
 * Created by luchao on 4/26/15.
 */
public class SparkVisitor extends TrackingVisitor {

    private List<GraphPipe> pipes;

    public SparkVisitor() {
        pipes = new ArrayList<GraphPipe>();
    }

    public GraphPipe getSparkPipe()
    {
        return pipes.get(pipes.size() - 1);
    }

    @Override
    protected void operateOn(PipelineNode pipeNode) {
        super.operateOn(pipeNode);
    }

    @Override
    public void visit(SnapNode snapNode) {
        Map<String, Object> map = snapNode.getRequestData();
        Object object = map.get("data_map");
        String line = object.toString();

        int start = line.indexOf("class_id") + 9;
        int end = line.indexOf(",", start);
        String type = line.substring(start, end);
        try {
            if (type.equals("com-snaplogic-snaps-hadoop-hdfsread")) {
                String value = "hdfs://localhost:9000/user/luchao/amplab_seq/text/tiny/rankings";
                GraphPipe source = new SourcePipe(value);
                pipes.add(source);
            } else if (type.equals("com-snaplogic-snaps-transform-csvparser")) {
                if(pipes.size() > 0) {
                    String[] columns = {"pageURL", "pageRank", "avgDuration"};
                    GraphPipe pre = pipes.get(pipes.size() - 1);
                    GraphPipe parser = new ParserPipe(pre, columns);
                    pipes.add(parser);
                }
            } else if (type.equals("com-snaplogic-snaps-flow-filter")) {
                if (pipes.size() > 0) {
                    GraphPipe pre = pipes.get(pipes.size() - 1);
                    GraphPipe filter = new OperationPipe("filter", pre);
                    pipes.add(filter);
                }
            } else if (type.equals("com-snaplogic-snaps-flow-aggragation")) {
                if (pipes.size() > 0) {
                    GraphPipe pre = pipes.get(pipes.size() - 1);
                    GraphPipe mapper = new OperationPipe("mapper", pre);
                    pipes.add(mapper);

                    GraphPipe reducer = new OperationPipe("reducer", mapper);
                    pipes.add(reducer);
                }
            } else if (type.equals("com-snaplogic-snaps-transform-csvformatter"))
            {
               if(pipes.size() > 0) {
                   GraphPipe pre = pipes.get(pipes.size() - 1);
                   GraphPipe format = new FormatterPipe(pre);
                   pipes.add(format);
               }
            } else if (type.equals("com-snaplogic-snaps-hadoop-hdfswrite"))
            {
               if(pipes.size() > 0) {
                   String value = "hdfs://localhost:9000/user/luchao/amplab_seq/result";
                   GraphPipe pre = pipes.get(pipes.size() - 1);
                   GraphPipe sink = new SinkPipe(value, pre);
                   pipes.add(sink);
               }
            }
        } catch (java.lang.Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getErrors() {
        return super.getErrors();
    }

    @Override
    public Set<String> getVisitedNodes() {
        return super.getVisitedNodes();
    }
}
