<?php

interface EarthIT_S3BlobRepo_HashScheme {
	public function newHashing();
	public function hashToFilePath( $hash );
	public function hashToUrn( $hash );
	public function urnToHash( $hash );
}

class EarthIT_S3BlobRepo_SHA1HashScheme implements EarthIT_S3BlobRepo_HashScheme {
	public static function getInstance() {
		return new self();
	}
	public function newHashing() {
		return new TOGoS_Hash_NativeHashing('sha1');
	}
	public function hashToFilePath( $hash ) {
		$b32 = TOGoS_Base32::encode($hash);
		return substr($b32,0,2).'/'.$b32;
	}
	public function hashToUrn( $hash ) {
		return "urn:sha1:".TOGoS_Base32::encode($hash);
	}
	public function urnToHash( $urn ) {
		if( preg_match('/^urn:(?:sha1|bitprint):([2-7A-Z]{32})/', $urn, $bif ) ) {
			return TOGoS_Base32::decode($bif[1]);
		}
		throw new Exception("Can't extract SHA-1 from this URN: '$urn'");
	}
}

class EarthIT_S3BlobRepo_S3Repository implements TOGoS_PHPN2R_Repository
{
	protected $s3Client;
	protected $s3BucketName;
	protected $s3RepoPath;
	public $sector = 'default';
	protected $hashScheme;
	
	public function __construct( $s3Client, $s3BucketName, $s3RepoPath, EarthIT_S3BlobRepo_HashScheme $hashScheme=null ) {
		$this->s3Client = $s3Client;
		$this->s3BucketName = $s3BucketName;
		$this->s3RepoPath = $s3RepoPath;
		if( $hashScheme === null ) $hashScheme = EarthIT_S3BlobRepo_SHA1HashScheme::getInstance();
		$this->hashScheme = $hashScheme;
	}
	
	protected function s3BlobPath( $hash ) {
		$prefix = $this->s3RepoPath ? "{$this->s3RepoPath}/" : '';
		return $prefix."data/{$this->sector}/".$this->hashScheme->hashToFilePath( $hash );
	}
	
	protected function uploadToS3($data, &$hash=null) {
		if( !$this->s3Client->doesBucketExist($this->s3BucketName) ) {
			throw new Exception("Can't post file to S3 because the '{$this->s3BucketName}' bucket doesn't exist.");
		}
		
		$data = (string)$data;
		
		$hashing = $this->hashScheme->newHashing();
		$hashing->update($data);
		$hash = $hashing->digest();
		
		$s3BlobPath = $this->s3BlobPath($hash);
		
		// If already on S3, don't need to re-upload
		if( $this->s3Client->doesObjectExist($this->s3BucketName, $s3BlobPath) ) return $s3BlobPath;
		
		$this->s3Client->upload($this->s3BucketName, $s3BlobPath, $data);
		return $s3BlobPath;
	}
	
	protected function putString( $data, $sector=null, $expectedUrn=null ) {
		$this->uploadToS3( $data, $hash );
		return $this->hashScheme->hashToUrn($hash);
	}
	
	public function putBlob( Nife_Blob $blob, array $options=array() ) {
		return $this->putString($blob);
	}
	
	public function putStream( $stream, $sector=null, $expectedUrn=null ) {
		$data = stream_get_contents($stream);
		return $this->putString($data, $sector, $expectedUrn);
	}
	
	public function getBlob( $urn ) {
		try {
			$hash = $this->hashScheme->urnToHash( $urn );
		} catch( Exception $e ) { return null; /* not one of ours, apparently! */ }

		try {
			$entity = $this->s3Client->getObject(array('Bucket'=>$this->s3BucketName, 'Key'=>$this->s3BlobPath($hash)));
		} catch( Aws\S3\Exception\NoSuchKeyException $e ) { return null; }
		return Nife_Util::blob( (string)($entity['Body']) );
	}
}
