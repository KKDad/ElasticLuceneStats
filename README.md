## ElasticLuceneStats

Quick program to dig deeper into an Elasticsearch node and pull out the stats on a per-field bases for each
grouping of indexes for the purposes of optimizing (in this case, reducing their size)

## Usage
~~~
gradlew shadowJar

$ java -jar LuceneStats-all.jar -?
usage: LuceneStats
 -d,--doc                Include a random document for stored fields.
 -n,--sampleSize <arg>   Number of documents to sample per segment, if
                         sampling is enabled. Defaults to 10000
 -s,--sample             Sample the lucene index and produce an estimated
                         size for stored fields.

$ java -jar LuceneStats-all.jar -d /d/elasticsearch/ag16-cdf-single.ad.interset.com/nodes/0/_state

~~~


## Example Output
~~~
Index Group: working_hours
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -> working_hours_0_2020-10-28_19_41_14; 1,200 docs;  0 deleted docs;  185,225 bytes;  154.35 bytes/doc  dir: ihf0aUoqT9-tlSwMYhjlKg
  -> working_hours_0_2020-10-29_02_45_31; 1,200 docs;  0 deleted docs;  184,977 bytes;  154.15 bytes/doc  dir: viEFndGVRQ-9iEBqzVdJTQ
  -> working_hours_0_2020-10-30_02_45_10; 1,200 docs;  0 deleted docs;  184,897 bytes;  154.08 bytes/doc  dir: oJ_aY-YFRcO5RyN3dVxb3g
  -> working_hours_0_2020-10-31_02_45_03; 1,200 docs;  0 deleted docs;  184,898 bytes;  154.08 bytes/doc  dir: XLijan3yTIedO-kt6QorsQ
  -> working_hours_0_2020-11-01_02_45_08; 1,200 docs;  0 deleted docs;  184,738 bytes;  153.95 bytes/doc  dir: Ze9t1gXyQzuNPQnO4kz5Zw
  -> working_hours_0_2020-11-02_02_45_32; 1,200 docs;  0 deleted docs;  184,847 bytes;  154.04 bytes/doc  dir: SDXT0X3zSsKQJEm46QrTQA
  -> working_hours_0_2020-11-03_02_45_12; 1,200 docs;  0 deleted docs;  185,068 bytes;  154.22 bytes/doc  dir: _mqqBdUaRQG0CLCtrvlxGw
  -> working_hours_0_2020-11-04_02_44_24; 1,200 docs;  0 deleted docs;  184,860 bytes;  154.05 bytes/doc  dir: epAYnTDESDqlAf2E9vIIEQ
  -> working_hours_0_2020-11-05_02_45_04; 1,200 docs;  0 deleted docs;  184,883 bytes;  154.07 bytes/doc  dir: RMZ_PXh6TYqJtj30dNRvvg
  -> working_hours_0_2020-11-06_02_44_58; 1,200 docs;  0 deleted docs;  190,723 bytes;  158.94 bytes/doc  dir: CtrFjHTbTUugw8JPCRMEiA
  -> working_hours_0_2020-11-16_21_31_16; 1,200 docs;  0 deleted docs;  184,966 bytes;  154.14 bytes/doc  dir: YpbLoY9GQyi-M_TesusspQ
  -> working_hours_0_2020-11-17_02_50_52; 1,200 docs;  0 deleted docs;  191,137 bytes;  159.28 bytes/doc  dir: y8LaVdGgREic7n8x1QYxyA
  -> working_hours_0_2020-11-18_02_57_32; 1,200 docs;  0 deleted docs;  189,425 bytes;  157.85 bytes/doc  dir: V7z9BdNZQ1i6wiW8Og6GGw
  -> working_hours_0_2020-11-19_02_57_32; 1,200 docs;  0 deleted docs;  190,655 bytes;  158.88 bytes/doc  dir: WBPtz-bkRCuxjHqZlnTXnw
  -> working_hours_0_2020-11-20_02_58_36; 1,200 docs;  0 deleted docs;  185,214 bytes;  154.35 bytes/doc  dir: iWQdVeOLSaeJArTrGoFdtw
  -> working_hours_0_2020-11-21_02_57_18; 1,200 docs;  0 deleted docs;  190,195 bytes;  158.50 bytes/doc  dir: f_pJ2ckOQA-4vaYw8xyQuw
  -> working_hours_0_2020-11-22_02_56_29; 1,200 docs;  0 deleted docs;  184,848 bytes;  154.04 bytes/doc  dir: ZV_pr-U2S3GHX-0KpK2lUw
  -> working_hours_0_2020-11-23_02_57_54; 1,200 docs;  0 deleted docs;  185,135 bytes;  154.28 bytes/doc  dir: EJKRU3_XRmWSTY7q8Hbefw
  -> working_hours_0_2020-11-24_02_57_53; 1,200 docs;  0 deleted docs;  184,867 bytes;  154.06 bytes/doc  dir: ZOa004pQQQ6PVnFlHad32g
  -> working_hours_0_2020-11-25_02_56_26; 1,200 docs;  0 deleted docs;  185,435 bytes;  154.53 bytes/doc  dir: rBsPzjWlSM-LBbpKJLXwKw
  -> working_hours_0_2020-12-08_16_30_59; 1,200 docs;  0 deleted docs;  184,851 bytes;  154.04 bytes/doc  dir: JWAhppKOT-uxypDC1ioY_w
  -> working_hours_0_2020-12-09_02_51_03; 1,200 docs;  0 deleted docs;  190,587 bytes;  158.82 bytes/doc  dir: 6_KWWlJiSIm7JgbrcsibKg
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Index Statistics: working_hours
 - # of Documents    :          26,400
 - # of Deleted Docs :               0
 - Overall Percentage:            5.18 %
 - Lucene Index      :       4,102,431 bytes
 - Lucene TransLog   :           3,146 bytes
 - Total Uncompressed:       4,102,431 bytes
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -> _id                                 (21.47%), DOCS                           Field 1,424,293 bytes; Stored=321,728; IndexBytes=3,168; Terms=26400; 12.19 bytes/term; TermBytes=321,728; BlockSuffixBytes=338,398; UncompressedBlockSuffixBytes=336,753; BlockStatsBytes=26,400; BlockOtherBytes=76,118
  -> _primary_term                       ( 0.00%), NONE
  -> _seq_no                             ( 0.00%), NONE
  -> _source                             (55.53%), NONE                           Field 3,684,472 bytes; Stored=3,684,472; IndexBytes=0;
  -> _version                            ( 0.00%), NONE
  -> entityHash                          ( 0.91%), DOCS                           Field 60,328 bytes; Stored=0; IndexBytes=3,168; Terms=1100; 15.92 bytes/term; TermBytes=17,512; BlockSuffixBytes=18,700; UncompressedBlockSuffixBytes=18,612; BlockStatsBytes=1,100; BlockOtherBytes=1,236
  -> entityName                          ( 0.61%), DOCS_AND_FREQS_AND_POSITIONS   Field 40,660 bytes; Stored=0; IndexBytes=3,168; Terms=1188; 8.44 bytes/term; TermBytes=10,032; BlockSuffixBytes=11,308; UncompressedBlockSuffixBytes=11,220; BlockStatsBytes=2,376; BlockOtherBytes=2,556
  -> entityName.raw                      ( 0.57%), DOCS                           Field 38,064 bytes; Stored=0; IndexBytes=3,168; Terms=1100; 9.16 bytes/term; TermBytes=10,076; BlockSuffixBytes=11,264; UncompressedBlockSuffixBytes=11,176; BlockStatsBytes=1,100; BlockOtherBytes=1,280
  -> entityType                          ( 0.07%), DOCS_AND_FREQS_AND_POSITIONS   Field 4,914 bytes; Stored=0; IndexBytes=3,168; Terms=88; 3.00 bytes/term; TermBytes=264; BlockSuffixBytes=396; UncompressedBlockSuffixBytes=352; BlockStatsBytes=218; BlockOtherBytes=516
  -> entityType.raw                      ( 0.07%), DOCS                           Field 4,652 bytes; Stored=0; IndexBytes=3,168; Terms=88; 3.00 bytes/term; TermBytes=264; BlockSuffixBytes=396; UncompressedBlockSuffixBytes=352; BlockStatsBytes=130; BlockOtherBytes=342
  -> expected                            ( 0.00%), NONE
  -> id                                  (20.76%), DOCS                           Field 1,377,394 bytes; Stored=0; IndexBytes=3,168; Terms=26400; 15.93 bytes/term; TermBytes=420,508; BlockSuffixBytes=425,985; UncompressedBlockSuffixBytes=424,361; BlockStatsBytes=26,400; BlockOtherBytes=76,972
  -> minute                              ( 0.00%), NONE
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


~~~