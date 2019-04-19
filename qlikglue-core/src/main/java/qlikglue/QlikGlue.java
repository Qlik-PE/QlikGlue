/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package qlikglue;


import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.apache.kafka.common.utils.Exit;
import org.apache.log4j.*;
import qlikglue.source.kafka.QlikGlueKafkaConsumer;

import java.io.IOException;

import static qlikglue.source.kafka.QlikGlueKafkaConsumer.argParser;
import static qlikglue.source.kafka.QlikGlueKafkaConsumer.createFromArgs;

public class QlikGlue {
    public static void main(String[] args) {
        //This is the root logger provided by log4j
        boolean append = false;
        String logfile = "/tmp/qlikglue.log";
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);

        //Define the log pattern layout
        PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %x - %m%n");

        //Add an appender to the root logger
        try {
            rootLogger.addAppender(new FileAppender(layout, logfile, append));
        } catch (IOException e) {
            rootLogger.addAppender(new ConsoleAppender(layout));
            rootLogger.error("failed to open log4j log file. Switching to ConsoleAppender.", e);
        }

        ArgumentParser parser = argParser();
        if (args.length == 0) {
            parser.printHelp();
            Exit.exit(0);
        }

        try {
            final QlikGlueKafkaConsumer consumer = createFromArgs(parser, args);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.close()));

            consumer.run();
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        }

    }
}
