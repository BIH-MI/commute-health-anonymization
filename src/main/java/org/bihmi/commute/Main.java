/**
 * Anonymization process for the commute health study
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bihmi.commute;

import org.apache.commons.cli.*;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataHandle;

import java.io.File;
import java.io.IOException;

/**
 * Main entry point
 */
public class Main {

    /** CLI parameter */
    private static final Option PARAMETER_INPUT_PATH = Option.builder("i").longOpt("input")
            .desc("Path to input file")
            .hasArg(true)
            .required(true)
            .build();

    /** CLI parameter */
    private static final Option PARAMETER_OUTPUT_PATH = Option.builder("o").longOpt("output")
            .desc("Path to output file")
            .hasArg(true)
            .required(true)
            .build();

    /**
     * Main entry point
     * @param args Should include input and output paths
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        // Prepare options
        Options options = new Options();

        // Prepare parsing
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        // Check args
        if (args == null || args.length == 0) {
            cliParameterHelp(options, "No parameters provided");
            return;
        }

        // Parse
        try {
            cmd = parser.parse(options, args, true);
        } catch (Exception e) {
            cliParameterHelp(options, e.getMessage());
            return;
        }

        // Parse again with specific options
        options = new Options();
        options.addOption(PARAMETER_INPUT_PATH);
        options.addOption(PARAMETER_OUTPUT_PATH);

        try {
            cmd = parser.parse(options, args, false);
        } catch (Exception e) {
            cliParameterHelp(options, e.getMessage());
            return;
        }

        // define Input and output file paths
        String inputPath = cmd.getOptionValue(PARAMETER_INPUT_PATH);
        String output = cmd.getOptionValue(PARAMETER_OUTPUT_PATH);
        if (!output.toLowerCase().endsWith(".csv")) {
        	output += ".csv";
        }

        // Anonymization
        Data data = IO.loadData(new File(inputPath));
        DataHandle anonymized = Anon.anonymizeCommuteData(data);
        IO.writeResult(anonymized, new File(output));
    }


    /**
     * Print help
     * @param options
     * @param message
     */
    private static void cliParameterHelp(Options options, String message) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -jar [file].jar", message, options, "");
    }
}
