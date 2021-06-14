package com.shf.calcite.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2021/6/4 1:18
 */
public class AbstractBaseSchema implements Schema {

    public AbstractBaseSchema() {
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    /**
     * Returns a map of tables in this schema by name.
     *
     * <p>The implementations of {@link #getTableNames()}
     * and {@link #getTable(String)} depend on this map.
     * The default implementation of this method returns the empty map.
     * Override this method to change their behavior.</p>
     *
     * @return Map of tables in this schema by name
     */
    protected Map<String, Table> getTableMap() {
        return ImmutableMap.of();
    }

    @Override
    public final Set<String> getTableNames() {
        return getTableMap().keySet();
    }

    @Override
    public Table getTable(String name) {
        return getTableMap().get(name);
    }

    /**
     * Returns a map of types in this schema by name.
     *
     * <p>The implementations of {@link #getTypeNames()}
     * and {@link #getType(String)} depend on this map.
     * The default implementation of this method returns the empty map.
     * Override this method to change their behavior.</p>
     *
     * @return Map of types in this schema by name
     */
    protected Map<String, RelProtoDataType> getTypeMap() {
        return ImmutableMap.of();
    }
    @Override
    public RelProtoDataType getType(String name) {
        return getTypeMap().get(name);
    }
    @Override
    public Set<String> getTypeNames() {
        return getTypeMap().keySet();
    }

    /**
     * Returns a multi-map of functions in this schema by name.
     * It is a multi-map because functions are overloaded; there may be more than
     * one function in a schema with a given name (as long as they have different
     * parameter lists).
     *
     * <p>The implementations of {@link #getFunctionNames()}
     * and {@link Schema#getFunctions(String)} depend on this map.
     * The default implementation of this method returns the empty multi-map.
     * Override this method to change their behavior.</p>
     *
     * @return Multi-map of functions in this schema by name
     */
    protected Multimap<String, Function> getFunctionMultimap() {
        return ImmutableMultimap.of();
    }
    @Override
    public final Collection<Function> getFunctions(String name) {
        return getFunctionMultimap().get(name); // never null
    }

    @Override
    public final Set<String> getFunctionNames() {
        return getFunctionMultimap().keySet();
    }

    /**
     * Returns a map of sub-schemas in this schema by name.
     *
     * <p>The implementations of {@link #getSubSchemaNames()}
     * and {@link #getSubSchema(String)} depend on this map.
     * The default implementation of this method returns the empty map.
     * Override this method to change their behavior.</p>
     *
     * @return Map of sub-schemas in this schema by name
     */
    protected Map<String, Schema> getSubSchemaMap() {
        return ImmutableMap.of();
    }

    @Override
    public final Set<String> getSubSchemaNames() {
        return getSubSchemaMap().keySet();
    }

    @Override
    public final Schema getSubSchema(String name) {
        return getSubSchemaMap().get(name);
    }

    /** Schema factory that creates an
     * {@link AbstractBaseSchema}. */
    public static class Factory implements SchemaFactory {
        public static final AbstractBaseSchema.Factory INSTANCE = new AbstractBaseSchema.Factory();

        private Factory() {}

        @Override
        public Schema create(SchemaPlus parentSchema, String name,
                             Map<String, Object> operand) {
            return new AbstractBaseSchema();
        }
    }

}
