package app;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.StrictErrorHandler;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.io.CharStreams;
import com.google.gson.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Main {

    static SparkSession spark = SparkSession
            .builder()
            .appName("java_spark_hive")
            .master("local[*]")
            .getOrCreate();

    private static FhirContext fhirContext;
    static AWSCredentials credentials = new BasicAWSCredentials(
            "AKIA2QZ47VDF5IQMZDEV ",
            "FXt6mxYscOaxG5pZcofcGiidMqhqzQ4USSFn5dBb"
    );

    static AmazonS3 s3client = AmazonS3ClientBuilder
            .standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_EAST_1)
            .build();

    public static void setFhirContext(FhirContext fhirContext) {
        Main.fhirContext = fhirContext;
    }

    public static void main(String[] args) throws IOException {
//        String filepath = args[0];
//        FileInputStream f = new FileInputStream(filepath);
//        String in = IOUtils.toString(f,"UTF-8");

        String in = s3proba();
        main2(in);
    }

    private static String s3proba() throws IOException {



        S3Object s3object = s3client.getObject("fhirprobajava", "source/hcp-epim_zmfe9s6fa4.json");
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        String input = null;
        try (Reader reader = new InputStreamReader(inputStream)) {
            input = CharStreams.toString(reader);
        }
        System.out.println(input);
        return input;

    }

    public static void main2(String in) throws IOException {
        fhirContext= FhirContext.forR4();
        fhirContext.setParserErrorHandler(new StrictErrorHandler());





        JsonObject finalRes = new JsonObject();
        new JsonParser();
        JsonObject json = JsonParser.parseString(in).getAsJsonObject();
        JsonElement epimId = json.get("_id");
        JsonObject source = json.get("_source").getAsJsonObject();

        JsonArray resIdenArray = new JsonArray();


        JsonArray entry = source
                .get("voyagerBundle").getAsJsonObject()
                .get("entry").getAsJsonArray();
        JsonArray resultIdentifier = new JsonArray();
        JsonArray practitioner = new JsonArray();
        JsonArray practitioner2 = new JsonArray();
        JsonArray name = new JsonArray();
        JsonArray identifier = null;
        for(int p = 0; p < entry.size(); p++){
            JsonElement iden = source
                    .get("voyagerBundle").getAsJsonObject()
                    .get("entry").getAsJsonArray().get(p).getAsJsonObject()
                    .get("resource").getAsJsonObject()
                    .get("identifier");


            JsonObject resPrac = new JsonObject();
            JsonObject resPrac2 = new JsonObject();
            JsonObject resName = new JsonObject();

            JsonObject resInedObject = new JsonObject();



            resPrac2.add("epimId", epimId);
            resPrac.add("epimId", epimId);
            if(iden == null){
                continue;
            }else{
                identifier = source
                        .get("voyagerBundle").getAsJsonObject()
                        .get("entry").getAsJsonArray().get(p).getAsJsonObject()
                        .get("resource").getAsJsonObject()
                        .get("identifier").getAsJsonArray();
            }
            for(int i = 0; i < identifier.size(); i++){
                JsonObject iter = identifier.get(i).getAsJsonObject();
                JsonElement sourceSystemCd = null;
                if(iter.get("extension") != null){
                    sourceSystemCd = iter
                            .get("extension").getAsJsonArray().get(0).getAsJsonObject()
                            .get("valueMetadata").getAsJsonObject()
                            .get("sourceSystemCd");
                }

                JsonElement modifiedDateTime = null;
                if(iter.get("extension") != null){
                    modifiedDateTime = iter
                            .get("extension").getAsJsonArray().get(0).getAsJsonObject()
                            .get("valueMetadata").getAsJsonObject()
                            .get("modifiedDateTime");
                }

                resInedObject.add("modifiedDateTime", modifiedDateTime);
                JsonElement periodStart = null;
                JsonElement periodEnd = null;
                if(iter.get("period") != null){
                    periodStart = iter
                            .get("period").getAsJsonObject()
                            .get("start");

                    periodEnd = iter
                            .get("period").getAsJsonObject()
                            .get("end");
                }

                resInedObject.add("periodStart", periodStart);
                resInedObject.add("periodEnd", periodEnd);

                JsonElement code = null;

                if(iter.get("type").getAsJsonObject().get("coding").getAsJsonArray().get(0).getAsJsonObject().get("code") != null){
                    code = iter
                            .get("type").getAsJsonObject()
                            .get("coding").getAsJsonArray().get(0).getAsJsonObject()
                            .get("code");
                }

                JsonElement system = null;

                if(iter.get("type").getAsJsonObject().get("coding").getAsJsonArray().get(0).getAsJsonObject().get("system") != null){
                    system = iter
                            .get("type").getAsJsonObject()
                            .get("coding").getAsJsonArray().get(0).getAsJsonObject()
                            .get("system");
                }
                resInedObject.add("system", system);

                JsonElement display = null;

                if(iter.get("type").getAsJsonObject().get("coding").getAsJsonArray().get(0).getAsJsonObject().get("display") != null){
                    display = iter
                            .get("type").getAsJsonObject()
                            .get("coding").getAsJsonArray().get(0).getAsJsonObject()
                            .get("display");
                }

                JsonElement value = iter.get("value");
                JsonElement state = null;

                resInedObject.add("value", value);
                if(iter.get("characteristic") != null){
                    JsonArray charIter = iter
                            .get("characteristic").getAsJsonArray();
                    for(int j = 0; j < charIter.size(); j++){
                        if(charIter.get(j).getAsJsonObject().get("name").getAsString().equals("State")){
                            state = charIter
                                    .get(j).getAsJsonObject()
                                    .get("value");
                        }
                    }

                }
                resInedObject.add("state", state);

                resInedObject.addProperty("providerType", "P");
                JsonElement active = null;

                if(source
                        .get("voyagerBundle").getAsJsonObject()
                        .get("entry").getAsJsonArray().get(p).getAsJsonObject()
                        .get("resource").getAsJsonObject() != null){
                    active = source
                            .get("voyagerBundle").getAsJsonObject()
                            .get("entry").getAsJsonArray().get(p).getAsJsonObject()
                            .get("resource").getAsJsonObject()
                            .get("active");
                }
                resPrac2.add("active", active);
                resPrac.add("active", active);
                resInedObject.add("active", active);
            }

            JsonArray qualification = source
                    .get("voyagerBundle").getAsJsonObject()
                    .get("entry").getAsJsonArray().get(0).getAsJsonObject()
                    .get("resource").getAsJsonObject()
                    .get("qualification").getAsJsonArray();



            for(int i = 0; i < qualification.size(); i++){
                JsonObject iter = qualification
                        .get(i).getAsJsonObject();

                JsonElement sourceSystemCd = null;
                JsonElement modifiedDateTime = null;


                if( iter
                        .get("extension").getAsJsonArray().get(0).getAsJsonObject()
                        .get("valueMetadata").getAsJsonObject() != null){

                    sourceSystemCd = iter
                            .get("extension").getAsJsonArray().get(0).getAsJsonObject()
                            .get("valueMetadata").getAsJsonObject()
                            .get("sourceSystemCd");

                    modifiedDateTime = iter
                            .get("extension").getAsJsonArray().get(0).getAsJsonObject()
                            .get("valueMetadata").getAsJsonObject()
                            .get("modifiedDateTime");
                }

                resInedObject.add("sourceSystemCd", sourceSystemCd);

                JsonElement code = null;

                if(iter
                        .get("code").getAsJsonObject()
                        .get("coding").getAsJsonArray().get(0).getAsJsonObject() != null){
                    code = iter
                            .get("code").getAsJsonObject()
                            .get("coding").getAsJsonArray().get(0).getAsJsonObject()
                            .get("code");
                }

                resInedObject.add("code", code);

                JsonElement display = null;

                if(iter
                        .get("code").getAsJsonObject()
                        .get("coding").getAsJsonArray().get(0).getAsJsonObject() != null){
                    display = iter
                            .get("code").getAsJsonObject()
                            .get("coding").getAsJsonArray().get(0).getAsJsonObject()
                            .get("display");
                }
                resInedObject.add("display", display);




                JsonElement periodStart = null;

                if(iter.get("period") != null){
                    periodStart = iter
                            .get("period").getAsJsonObject()
                            .get("start");
                }

                JsonElement periodEnd = null;

                if(iter.get("period") != null){
                    periodStart = iter
                            .get("period").getAsJsonObject()
                            .get("end");
                }



            }


            JsonElement gender = source
                    .get("voyagerBundle").getAsJsonObject()
                    .get("entry").getAsJsonArray().get(0).getAsJsonObject()
                    .get("resource").getAsJsonObject()
                    .get("gender");


            resPrac2.add("gender", gender);
            resPrac.add("gender", gender);


            JsonArray nameSource = source
                    .get("voyagerBundle").getAsJsonObject()
                    .get("entry").getAsJsonArray().get(0).getAsJsonObject()
                    .get("resource").getAsJsonObject()
                    .get("name").getAsJsonArray();
            for(int i = 0; i < nameSource.size(); i++){

                JsonObject iter = nameSource.get(i).getAsJsonObject();
                JsonElement use = iter.get("use");
                resName.add("use", use);
                resInedObject.add("use", use);
                JsonElement given = iter.get("given");
                JsonElement family = iter.get("family");
                resName.add("family", family);
                JsonElement sourceSystemCd = iter.get("extension").getAsJsonArray().get(0)
                        .getAsJsonObject().get("valueMetadata").getAsJsonObject()
                        .get("sourceSystemCd");


                resName.add("given", given);

                resName.add("sourceSystemCd", sourceSystemCd);
            }


            JsonElement active = source.get("active");

            JsonElement birthDate = source.get("birthDate");
            resPrac2.add("birthDate", birthDate);
            resPrac.add("birthDate", birthDate);

            JsonElement resourceType = source.get("resourceType");


            resIdenArray.add(resInedObject);
            resPrac.add("identifier",resIdenArray);
            practitioner.add(resPrac);

            practitioner2.add(resPrac2);
            name.add(resName);

        }


        finalRes.add("Practitioner", practitioner);
        finalRes.add("Practitioner2", practitioner2);
        finalRes.add("name", name);


        try (Writer writer = new FileWriter("Output.json")) {
            Gson gson = new GsonBuilder().create();
            gson.toJson(finalRes, writer);
        }
        s3client.putObject(
                "fhirprobajava",
                "output/Output.json",
                new File("./Output.json")
        );


        Dataset<Row> df = spark.read().json("file:///home/sysopscdo2/vince-workspace/kafka/demohapi-fhir/target/Output.json");

        df.select("Practitioner").printSchema();

        Dataset<Row> df1 = flattenJSONdf(df.select("Practitioner"));

        df1.show();

    }

    private static Dataset flattenJSONdf(Dataset<Row> ds) {

        StructField[] fields = ds.schema().fields();

        List<String> fieldsNames = new ArrayList<>();
        for (StructField s : fields) {
            fieldsNames.add(s.name());
        }

        for (int i = 0; i < fields.length; i++) {

            StructField field = fields[i];
            DataType fieldType = field.dataType();
            String fieldName = field.name();

            if (fieldType instanceof ArrayType) {
                List<String> fieldNamesExcludingArray = new ArrayList<String>();
                for (String fieldName_index : fieldsNames) {
                    if (!fieldName.equals(fieldName_index))
                        fieldNamesExcludingArray.add(fieldName_index);
                }

                List<String> fieldNamesAndExplode = new ArrayList<>(fieldNamesExcludingArray);
                String s = String.format("explode_outer(%s) as %s", fieldName, fieldName);
                fieldNamesAndExplode.add(s);

                String[]  exFieldsWithArray = new String[fieldNamesAndExplode.size()];
                Dataset exploded_ds = ds.selectExpr(fieldNamesAndExplode.toArray(exFieldsWithArray));

                // explodedDf.show();

                return flattenJSONdf(exploded_ds);

            }
            else if (fieldType instanceof StructType) {

                String[] childFieldnames_struct = ((StructType) fieldType).fieldNames();

                List<String> childFieldnames = new ArrayList<>();
                for (String childName : childFieldnames_struct) {
                    childFieldnames.add(fieldName + "." + childName);
                }

                List<String> newfieldNames = new ArrayList<>();
                for (String fieldName_index : fieldsNames) {
                    if (!fieldName.equals(fieldName_index))
                        newfieldNames.add(fieldName_index);
                }

                newfieldNames.addAll(childFieldnames);

                List<Column> renamedStrutctCols = new ArrayList<>();

                for(String newFieldNames_index : newfieldNames){
                    renamedStrutctCols.add( new Column(newFieldNames_index.toString()).as(newFieldNames_index.toString().replace(".", "_")));
                }

                Seq renamedStructCols_seq = JavaConverters.collectionAsScalaIterableConverter(renamedStrutctCols).asScala().toSeq();

                Dataset ds_struct = ds.select(renamedStructCols_seq);

                return flattenJSONdf(ds_struct);
            }
            else{

            }

        }
        return ds;
    }
}

