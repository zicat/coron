/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.rules.materialize;

import com.google.common.collect.BiMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class AliasMaterializedViewJoinRule<C extends MaterializedViewJoinRule.Config>
        extends MaterializedViewJoinRule<C> {

    /**
     * Creates a MaterializedViewJoinRule.
     *
     * @param config
     */
    AliasMaterializedViewJoinRule(C config) {
        super(config);
    }

    @Override
    protected @Nullable RelNode rewriteView(
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            RexSimplify simplify,
            RelMetadataQuery mq,
            MatchModality matchModality,
            boolean unionRewriting,
            RelNode input,
            @Nullable Project topProject,
            RelNode node,
            @Nullable Project topViewProject,
            RelNode viewNode,
            BiMap<RexTableInputRef.RelTableRef, RexTableInputRef.RelTableRef>
                    queryToViewTableMapping,
            EquivalenceClasses queryEC) {
        List<RexNode> exprs =
                topProject == null ? extractReferences(rexBuilder, node) : topProject.getProjects();
        List<RexNode> exprsLineage = new ArrayList<>(exprs.size());
        for (RexNode expr : exprs) {
            Set<RexNode> s = mq.getExpressionLineage(node, expr);
            if (s == null) {
                // Bail out
                return null;
            }
            assert s.size() == 1;
            // Rewrite expr. Take first element from the corresponding equivalence class
            // (no need to swap the table references following the table mapping)
            exprsLineage.add(
                    RexUtil.swapColumnReferences(
                            rexBuilder, s.iterator().next(), queryEC.getEquivalenceClassesMap()));
        }
        List<RexNode> viewExprs =
                topViewProject == null
                        ? extractReferences(rexBuilder, viewNode)
                        : topViewProject.getProjects();
        List<RexNode> rewrittenExprs =
                rewriteExpressions(
                        rexBuilder,
                        mq,
                        input,
                        viewNode,
                        viewExprs,
                        queryToViewTableMapping.inverse(),
                        queryEC,
                        true,
                        exprsLineage);
        if (rewrittenExprs == null) {
            return null;
        }
        RelDataType relDataType = topProject != null ? topProject.getRowType() : node.getRowType();
        return relBuilder
                .push(input)
                .project(rewrittenExprs, relDataType.getFieldNames())
                .convert(relDataType, false)
                .build();
    }
}
