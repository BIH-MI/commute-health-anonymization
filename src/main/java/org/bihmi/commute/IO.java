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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataHandle;
import org.deidentifier.arx.DataHandleOutput;
import org.deidentifier.arx.DataSource;
import org.deidentifier.arx.DataType;
import org.deidentifier.arx.io.CSVDataOutput;

/**
 * IO-specific configuration
 */
public class IO {

    /** Column name for the type of commute from school to home*/
    public static final String FIELD_COMMUTE_FROM_SCHOOL = "CommHome";

    /** Column name for the type of commute from home to school*/
    public static final String FIELD_COMMUTE_TO_SCHOOL = "CommToSch";

    /** Column name for the measured distance from home to school in meter*/
    public static final String FIELD_DISTANCE_TO_SCHOOL = "DistFromHome";

    /** Column name for the measured distance from school to home in meter*/
    public static final String FIELD_DISTANCE_FROM_SCHOOL = "DistFromSchool";

    /** Column name for moderate-to vigorous-intensity physical activity*/
    public static final String FIELD_MVPA_SQRT = "MVPAsqrt";

    /** Column name for the maximum volume of oxygen the person could process*/
    public static final String FIELD_VO2_MAX = "VO2max";

    /** Column name for age*/
    public static final String FIELD_AGE = "age";

    /** Column name for gender*/
    public static final String FIELD_GENDER = "gender";

    /**
     * Load the input file
     * @param inputFile
     * @return
     * @throws IOException
     */
    public static Data loadData(File inputFile) throws IOException {
    	
		DataSource sourceSpecification = DataSource.createCSVSource(inputFile, StandardCharsets.UTF_8, ',', true);
		sourceSpecification.addColumn(FIELD_COMMUTE_FROM_SCHOOL, DataType.STRING);
		sourceSpecification.addColumn(FIELD_COMMUTE_TO_SCHOOL, DataType.STRING);
		sourceSpecification.addColumn(FIELD_DISTANCE_TO_SCHOOL, DataType.INTEGER);
		sourceSpecification.addColumn(FIELD_DISTANCE_FROM_SCHOOL, DataType.INTEGER);
		sourceSpecification.addColumn(FIELD_MVPA_SQRT, DataType.DECIMAL);
		sourceSpecification.addColumn(FIELD_VO2_MAX, DataType.DECIMAL);
		sourceSpecification.addColumn(FIELD_AGE, DataType.DECIMAL);
		sourceSpecification.addColumn(FIELD_GENDER, DataType.STRING);
		return Data.create(sourceSpecification);
    }

    /**
     * Writes the data
     * @param result
     * @param output
     * @throws IOException
     */
    public static void writeResult(DataHandle result, File output) throws IOException {
        
        // Filter out suppressed rows
        Iterator<String[]> iter = result.iterator();
        List<String[]> rows = new ArrayList<>();
        rows.add(iter.next());
        int rowNumber = 0;

        // Convert
        while (iter.hasNext()) {
            String[] row = iter.next();
            if (!(result instanceof DataHandleOutput) || !result.isOutlier(rowNumber)) {
                rows.add(row);
            }
            rowNumber++;
        }
        
        CSVDataOutput writer = new CSVDataOutput(output, ',');
        writer.write(rows.iterator());
    }
}