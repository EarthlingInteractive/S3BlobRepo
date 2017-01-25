<?php

class EarthIT_S3BlobRepo_S3RepositoryTest extends PHPUnit_Framework_TestCase
{
	protected $S3C;
	protected $S3BR;
	protected $s3BucketName;
	protected $s3RepoPath;
	protected $sector = 'default';
	protected $createdS3Files = array();
	
	public function setUp() {
		if( !file_exists('config/amazon.json') ) {
			$this->markTestSkipped("No 'amazon' config!");
		}
		$config = json_decode(file_get_contents('config/amazon.json'), true);
		if( $config === false ) {
			$this->markTestSkipped("Failed to parse config/amazon.json");
		}
		$this->s3BucketName = $config['testBucketName'];
		$this->s3RepoPath   = $config['testRepoPath'];
		$this->S3C = Aws\S3\S3Client::factory($config);
		$this->S3BR = new EarthIT_S3BlobRepo_S3Repository( $this->S3C, $this->s3BucketName, $this->s3RepoPath );
		$this->S3BR->sector = $this->sector;
	}
	
	public function tearDown() {
		foreach( $this->createdS3Files as $f ) {
			$this->S3C->deleteObject(array('Bucket'=>$this->s3BucketName, 'Key'=>$f));
		}
	}
	
	public function testUpload() {
		$data = "Hello, world! ".rand(1000000,9999999).rand(1000000,9999999).rand(1000000,9999999);
		$base32Sha1 = TOGoS_PHPN2R_Base32::encode(sha1($data,true));
		$first2 = substr($base32Sha1,0,2);
		$s3Path = "{$this->s3RepoPath}/data/{$this->sector}/$first2/$base32Sha1";
		$this->createdS3Files[] = $s3Path;
		
		$urn = $this->S3BR->putBlob( Nife_Util::blob($data) );
		$this->assertTrue( $this->S3C->doesObjectExist($this->s3BucketName, $s3Path) );
		
		$fetchedData = $this->S3BR->getBlob($urn);
		$this->assertEquals( $data, (string)$fetchedData );
	}
}
