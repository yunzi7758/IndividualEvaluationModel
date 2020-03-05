package cep.groovy;

import org.apache.flink.cep.pattern.Pattern;

public interface InjectPattern {

    Pattern getPattern(String templateStr);

    boolean test(String templateStr);
}
