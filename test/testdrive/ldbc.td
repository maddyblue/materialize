# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the cardinality of LDBC tables.

! CREATE SOURCE gen FROM LOAD GENERATOR LDBC
contains: multi-output sources require a FOR TABLES (..) or FOR ALL TABLES statement

! CREATE SOURCE gen FROM LOAD GENERATOR LDBC (SCALE FACTOR 2) FOR ALL TABLES
contains: unsupported scale factor: 2

! CREATE SOURCE gen FROM LOAD GENERATOR LDBC (SCALE FACTOR -1) FOR ALL TABLES
contains: unsupported scale factor: -1

> CREATE SOURCE gen FROM LOAD GENERATOR LDBC (SCALE FACTOR 1) FOR ALL TABLES

 > SELECT 'Comment', count(*) FROM "Comment"
  UNION SELECT 'Comment_hasTag_Tag', count(*) FROM "Comment_hasTag_Tag"
  UNION SELECT 'Forum', count(*) FROM "Forum"
  UNION SELECT 'Forum_hasMember_Person', count(*) FROM "Forum_hasMember_Person"
  UNION SELECT 'Forum_hasTag_Tag', count(*) FROM "Forum_hasTag_Tag"
  UNION SELECT 'Organisation', count(*) FROM "Organisation"
  UNION SELECT 'Person', count(*) FROM "Person"
  UNION SELECT 'Person_hasInterest_Tag', count(*) FROM "Person_hasInterest_Tag"
  UNION SELECT 'Person_knows_Person', count(*) FROM "Person_knows_Person"
  UNION SELECT 'Person_likes_Comment', count(*) FROM "Person_likes_Comment"
  UNION SELECT 'Person_likes_Post', count(*) FROM "Person_likes_Post"
  UNION SELECT 'Person_studyAt_University', count(*) FROM "Person_studyAt_University"
  UNION SELECT 'Person_workAt_Company', count(*) FROM "Person_workAt_Company"
  UNION SELECT 'Place', count(*) FROM "Place"
  UNION SELECT 'Post', count(*) FROM "Post"
  UNION SELECT 'Post_hasTag_Tag', count(*) FROM "Post_hasTag_Tag"
  UNION SELECT 'Tag', count(*) FROM "Tag"
  UNION SELECT 'TagClass', count(*) FROM "TagClass"
  ORDER BY 1
Comment 1739438
Comment_hasTag_Tag 2176131
Forum 100827
Forum_hasMember_Person 2909768
Forum_hasTag_Tag 328584
Organisation 7954
Person 10295
Person_hasInterest_Tag 238052
Person_knows_Person 173014
Person_likes_Comment 1109813
Person_likes_Post 760455
Person_studyAt_University 8309
Person_workAt_Company 22044
Place 1459
Post 1121226
Post_hasTag_Tag 751933
Tag 16079
TagClass 70
