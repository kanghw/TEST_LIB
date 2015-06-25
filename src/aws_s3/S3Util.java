/** 
 * @file    S3Util.java
 * @brief   
 * @author  
 * @author  KHW
 * @date    create : 2015. 6. 25.
 * @date    modify : 2015. 6. 25.
 */
package aws_s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/** 
 * @brief   S3Util
 * @author  
 * @author  KHW
 * @date    create : 2015. 6. 25.
 * @date    modify : 2015. 6. 25.
 */
public class S3Util {
    private static final String FOLDER_SEPERATOR = "/";
    
    /**
     * @brief return AmazonS3 client 
     * @details 
     * @param  String endPointUrl - S3 URL path.
     * @return AmazonS3
     * @throws
     */
    public static AmazonS3 connectAmazonS3(AWSCredentials credentials, String endPointUrl){
        AmazonS3 s3 = new AmazonS3Client(credentials);
        s3.setEndpoint(endPointUrl);
        return s3;
    }
    
    /**
     * @brief return AWSCredentials
     * @details 기본 경로에 저장된 credentials 파일에 저장된 정보를 바탕으로 AWSCredentials 를 반환한다. 
     * @param  
     * @return AWSCredentials
     * @throws
     */
    public static AWSCredentials getAWSCredentials(){
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct " + "location (~/.aws/credentials), and is in valid format.", e);
        }
        return credentials;
    }
    
    /**
     * @brief create folder from bucket path.
     * @details 
     * @param  
     * AmazonS3 s3 - S3 client
     * String bucketName
     * String folderPath - exclude bucket path
     * @return void
     * @throws
     */
    public static void createFolder(AmazonS3 s3, String bucketName, String folderPath){
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        InputStream emptyContents = new ByteArrayInputStream(new byte[0]);
        
        PutObjectRequest folderRequest = new PutObjectRequest(bucketName, getFolderPath(folderPath), emptyContents, metadata);
        s3.putObject(folderRequest);
    }
    
    /**
     * @brief check the key from bucket
     * @details  
     * @param  
     * AmazonS3 s3  -  S3 client
     * String bucketName
     * String keyName
     * @return boolean
     * @throws
     */
    public static boolean keyExist(AmazonS3 s3, String bucketName, String key){
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
            key = getLeftSlashAdd(key);
            listObjectsRequest.withPrefix(key);
            ObjectListing objects = null;
            do {
                objects = s3.listObjects(listObjectsRequest);
                for (S3ObjectSummary obj : objects.getObjectSummaries()) {
                    if(StringUtils.isNotEmpty(obj.getKey()) && obj.getKey().equals(key)){
                        return true;
                    }
                }
                listObjectsRequest.setMarker(objects.getNextMarker());
            } while (objects.isTruncated());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, " + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (Exception e) {
            System.out.println(ExceptionUtils.getStackTrace(e));
        }
        return false;
    }
    
    /**
     * @brief folderPath를 입력해서 S3 에서 사용하는 폴더 형태로 변환한 후 반환.
     * @details 
     * @param  S3Util
     * @return String
     * @throws 
     */
    private static String getFolderPath(String folerPath) {
        String result = folerPath + FOLDER_SEPERATOR;
        while(result.indexOf(FOLDER_SEPERATOR + FOLDER_SEPERATOR) >= 0 ){
            result = result.replaceAll(FOLDER_SEPERATOR + FOLDER_SEPERATOR, FOLDER_SEPERATOR);
        }
        result = getLeftSlashTrim(folerPath);
        return result;
    }

    /**
     * @throws FileNotFoundException 
     * @brief 폴더를 삭제한다.  
     * @details 경로를 입력하면 최종 경로만 삭제한다.
     * @param  
     * AmazonS3 s3  -  S3 client
     * String bucketName  - 사용할 버킷 명.
     * String path  -  삭제할 경로.(버킷을 제외한 절대경로 입력)
     * @return void
     * @throws
     */
    public static void deleteFolder(AmazonS3 s3, String bucketName, String path) throws FileNotFoundException{
        path = getFolderPath(path);
        if(keyExist(s3, bucketName, path)){
            s3.deleteObject(bucketName, path + FOLDER_SEPERATOR);
        }else{
            throw new FileNotFoundException("Directory <" + path + "> is not Exist!");
        }
    }
    
    /**
     * @throws FileAlreadyExistsException 
     * @brief 파일을 특정 버킷에 업로드 한다. 
     * @details 파일 객체를 받아서 특정 버킷과 패스에 업로드한다.
     * @param  
     * AmazonS3 s3  -  S3 client
     * String bucketName  - 사용할 버킷 명.
     * String keyName  -  생성할 파일의 경로 및 파일명 (버킷을 제외한 절대경로 입력)
     * File file = 업로드할 파일.
     * @return void
     * @throws
     */
    public static void uploadFile(AmazonS3 s3, String bucketName, String keyName, File file) throws FileAlreadyExistsException{
        if(keyExist(s3, bucketName, keyName)){
            throw new FileAlreadyExistsException(bucketName + ":" + keyName + " is already Exist!");
        }
        keyName = getLeftSlashTrim(keyName);
        s3.putObject(bucketName, keyName, file);
    }
    
    /**
     * @throws FileNotFoundException 
     * @brief 파일을 특정 버킷에서 삭제한다. 
     * @details 버킷 명과 경로를 입력받아서 S3 내부의 파일을 삭제한다.
     * @param  
     * AmazonS3 s3  -  S3 client
     * String bucketName  - 사용할 버킷 명.
     * String path  -  삭제할 파일의 경로. (버킷을 제외한 절대경로 입력)
     * @return void
     * @throws
     */
    public static void deleteFile(AmazonS3 s3, String bucketName, String key) throws FileNotFoundException{
        if(keyExist(s3, bucketName, key)){
            System.out.println("delete path -> " + key);
            s3.deleteObject(bucketName, key);
        }else{
            throw new FileNotFoundException(key + " is not exists!");
        }
    }
    
    /**
     * @brief 특정 버킷에 있는 파일의 다운로드 URL 을 추출하여 반환한다. 
     * @details 
     * @param  
     * AmazonS3 s3  -  S3 client
     * String bucketName  - 사용할 버킷 명.
     * String path  -  다운로드 경로를 받을 파일 경로. (버킷을 제외한 절대경로 입력)
     * @return URL
     * @throws
     */
    public static URL getDownloadUrl(AmazonS3 s3, String bucketName, String key){
        key = getLeftSlashTrim(key);
        return s3.generatePresignedUrl(new GeneratePresignedUrlRequest(bucketName, key));
    }
    
    private static String getLeftSlashTrim(String key){
        String result = key;
        while(result.indexOf(FOLDER_SEPERATOR) == 0){
            result = result.substring(1);
        }
        return result;
    }
    
    private static String getLeftSlashAdd(String key){
        String result = key;
        if(key.indexOf(FOLDER_SEPERATOR) != 0){
            result = FOLDER_SEPERATOR + result;
        }
        return result;
    }
    
    public static void printList(AmazonS3 s3, String bucketName) throws IOException {
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);

            int fileCount = 0;
            int folderCount = 0;
            long totalSize = 0;
            ObjectListing objects = null;
            do {
                objects = s3.listObjects(listObjectsRequest);
                for (S3ObjectSummary obj : objects.getObjectSummaries()) {
                    if (obj.getSize() == 0) {
                        folderCount++;
                    } else {
                        fileCount++;
                        totalSize += obj.getSize();
                    }
                }
                listObjectsRequest.setMarker(objects.getNextMarker());
            } while (objects.isTruncated());
            
            System.out.println("FOLDER COUNT -> " + folderCount);
            System.out.println("FILE COUNT -> " + fileCount);
            System.out.println("TOTAL SIZE -> " + totalSize + " = " + getSize(totalSize));
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, " + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    
    private static String getSize(long size){
        String result = Long.toString(size);
        if(1024 <= size && size < Math.pow(1024, 2)){
            result = Math.round((size / 1024d) * 100) / 100d + " Kb";
        }
        if( Math.pow(1024, 2) <= size && size < Math.pow(1024, 3) ){
            result = Math.round((size / Math.pow(1024, 2) * 100)) / 100d + " Mb";
        }
        if( Math.pow(1024, 3) <= size && size < Math.pow(1024, 4) ){
            result = Math.round((size / Math.pow(1024, 3) * 100)) / 100d + " Gb";
        }
        if( Math.pow(1024, 4) <= size ){
            result = Math.round((size / Math.pow(1024, 4) * 100)) / 100d + " Tb";
        }
        return result;
    }
}