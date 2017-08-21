package com.bohaisoft.dal.client.spring.mapper;

import com.bohaisoft.dal.client.spring.DalSqlSessionDaoSupport;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.session.Configuration;
import org.springframework.beans.factory.FactoryBean;

import static org.springframework.util.Assert.notNull;

/**
 * Created by liuhb on 2017/1/5.
 */
public class DalMapperFactoryBean<T> extends DalSqlSessionDaoSupport implements FactoryBean<T> {


    private Class<T> mapperInterface;

    private boolean addToConfig = true;

    /**
     * Sets the mapper interface of the MyBatis mapper
     *
     * @param mapperInterface class of the interface
     */
    public void setMapperInterface(Class<T> mapperInterface) {
        this.mapperInterface = mapperInterface;
    }

    /**
     * If addToConfig is false the mapper will not be added to MyBatis. This means
     * it must have been included in mybatis-config.xml.
     * <p>
     * If it is true, the mapper will be added to MyBatis in the case it is not already
     * registered.
     * <p>
     * By default addToCofig is true.
     *
     * @param addToConfig
     */
    public void setAddToConfig(boolean addToConfig) {
        this.addToConfig = addToConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void checkDaoConfig() {
        super.checkDaoConfig();

        notNull(this.mapperInterface, "Property 'mapperInterface' is required");

        Configuration configuration = getSqlSession().getConfiguration();
        if (this.addToConfig && !configuration.hasMapper(this.mapperInterface)) {
            try {
                configuration.addMapper(this.mapperInterface);
            } catch (Throwable t) {
                logger.error("Error while adding the mapper '" + this.mapperInterface + "' to configuration.", t);
                throw new IllegalArgumentException(t);
            } finally {
                ErrorContext.instance().reset();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public T getObject() throws Exception {
        return getSqlSession().getMapper(this.mapperInterface);
    }

    /**
     * {@inheritDoc}
     */
    public Class<T> getObjectType() {
        return this.mapperInterface;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isSingleton() {
        return true;
    }
}
