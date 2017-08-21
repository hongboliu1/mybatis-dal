package com.bohaisoft.dal.client.spring.mapper;

import com.bohaisoft.dal.client.spring.DalSqlSessionTemplate;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.ClassPathBeanDefinitionScanner;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Set;

/**
 * Created by liuhb on 2017/1/5.
 */
public class DalClassPathMapperScanner extends ClassPathBeanDefinitionScanner {

    private boolean addToConfig = true;

    private SqlSessionFactory sqlSessionFactory;

    private DalSqlSessionTemplate dalSqlSessionTemplate;

    private String dalSqlSessionTemplateBeanName;

    private String sqlSessionFactoryBeanName;

    private Class<? extends Annotation> annotationClass;

    private Class<?> markerInterface;

    public DalClassPathMapperScanner(BeanDefinitionRegistry registry) {
        super(registry, false);
    }

    public void setAddToConfig(boolean addToConfig) {
        this.addToConfig = addToConfig;
    }

    public void setAnnotationClass(Class<? extends Annotation> annotationClass) {
        this.annotationClass = annotationClass;
    }

    public void setMarkerInterface(Class<?> markerInterface) {
        this.markerInterface = markerInterface;
    }

    public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
        this.sqlSessionFactory = sqlSessionFactory;
    }

    public void setDalSqlSessionTemplate(DalSqlSessionTemplate dalSqlSessionTemplate) {
        this.dalSqlSessionTemplate = dalSqlSessionTemplate;
    }

    public void setDalSqlSessionTemplateBeanName(String dalSqlSessionTemplateBeanName) {
        this.dalSqlSessionTemplateBeanName = dalSqlSessionTemplateBeanName;
    }

    public void setSqlSessionFactoryBeanName(String sqlSessionFactoryBeanName) {
        this.sqlSessionFactoryBeanName = sqlSessionFactoryBeanName;
    }

    /**
     * Configures parent scanner to search for the right interfaces. It can search
     * for all interfaces or just for those that extends a markerInterface or/and
     * those annotated with the annotationClass
     */
    public void registerFilters() {
        boolean acceptAllInterfaces = true;

        // if specified, use the given annotation and / or marker interface
        if (this.annotationClass != null) {
            addIncludeFilter(new AnnotationTypeFilter(this.annotationClass));
            acceptAllInterfaces = false;
        }

        // override AssignableTypeFilter to ignore matches on the actual marker interface
        if (this.markerInterface != null) {
            addIncludeFilter(new AssignableTypeFilter(this.markerInterface) {
                @Override
                protected boolean matchClassName(String className) {
                    return false;
                }
            });
            acceptAllInterfaces = false;
        }

        if (acceptAllInterfaces) {
            // default include filter that accepts all classes
            addIncludeFilter(new TypeFilter() {
                public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
                    return true;
                }
            });
        }

        // exclude package-info.java
        addExcludeFilter(new TypeFilter() {
            public boolean match(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
                String className = metadataReader.getClassMetadata().getClassName();
                return className.endsWith("package-info");
            }
        });
    }

    /**
     * Calls the parent search that will search and register all the candidates.
     * Then the registered objects are post processed to set them as
     * MapperFactoryBeans
     */
    @Override
    public Set<BeanDefinitionHolder> doScan(String... basePackages) {
        Set<BeanDefinitionHolder> beanDefinitions = super.doScan(basePackages);

        if (beanDefinitions.isEmpty()) {
            logger.warn("No MyBatis mapper was found in '" + Arrays.toString(basePackages) + "' package. Please check your configuration.");
        } else {
            for (BeanDefinitionHolder holder : beanDefinitions) {
                GenericBeanDefinition definition = (GenericBeanDefinition) holder.getBeanDefinition();

                if (logger.isDebugEnabled()) {
                    logger.debug("Creating MapperFactoryBean with name '" + holder.getBeanName()
                            + "' and '" + definition.getBeanClassName() + "' mapperInterface");
                }

                // the mapper interface is the original class of the bean
                // but, the actual class of the bean is MapperFactoryBean
                definition.getPropertyValues().add("mapperInterface", definition.getBeanClassName());
                definition.setBeanClass(DalMapperFactoryBean.class);

                definition.getPropertyValues().add("addToConfig", this.addToConfig);

                boolean explicitFactoryUsed = false;
                if (StringUtils.hasText(this.sqlSessionFactoryBeanName)) {
                    definition.getPropertyValues().add("sqlSessionFactory", new RuntimeBeanReference(this.sqlSessionFactoryBeanName));
                    explicitFactoryUsed = true;
                } else if (this.sqlSessionFactory != null) {
                    definition.getPropertyValues().add("sqlSessionFactory", this.sqlSessionFactory);
                    explicitFactoryUsed = true;
                }

                if (StringUtils.hasText(this.dalSqlSessionTemplateBeanName)) {
                    if (explicitFactoryUsed) {
                        logger.warn("Cannot use both: sqlSessionTemplate and sqlSessionFactory together. sqlSessionFactory is ignored.");
                    }
                    definition.getPropertyValues().add("dalSqlSessionTemplate", new RuntimeBeanReference(this.dalSqlSessionTemplateBeanName));
                    explicitFactoryUsed = true;
                } else if (this.dalSqlSessionTemplate != null) {
                    if (explicitFactoryUsed) {
                        logger.warn("Cannot use both: sqlSessionTemplate and sqlSessionFactory together. sqlSessionFactory is ignored.");
                    }
                    definition.getPropertyValues().add("dalSqlSessionTemplate", this.dalSqlSessionTemplate);
                    explicitFactoryUsed = true;
                }

                if (!explicitFactoryUsed) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Enabling autowire by type for MapperFactoryBean with name '" + holder.getBeanName() + "'.");
                    }
                    definition.setAutowireMode(AbstractBeanDefinition.AUTOWIRE_BY_TYPE);
                }
            }
        }

        return beanDefinitions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
        return (beanDefinition.getMetadata().isInterface() && beanDefinition.getMetadata().isIndependent());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean checkCandidate(String beanName, BeanDefinition beanDefinition) throws IllegalStateException {
        if (super.checkCandidate(beanName, beanDefinition)) {
            return true;
        } else {
            logger.warn("Skipping MapperFactoryBean with name '" + beanName
                    + "' and '" + beanDefinition.getBeanClassName() + "' mapperInterface"
                    + ". Bean already defined with the same name!");
            return false;
        }
    }
}
