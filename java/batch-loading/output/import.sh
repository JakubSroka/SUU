#!/bin/sh
neo4j-admin import --database social2.db --nodes organisation_0_0.csv --nodes place_0_0.csv --nodes tag_0_0.csv --nodes tagclass_0_0.csv --relationships organisation_isLocatedIn_place_0_0.csv --relationships place_isPartOf_place_0_0.csv --relationships tag_hasType_tagclass_0_0.csv --relationships tagclass_isSubclassOf_tagclass_0_0.csv --nodes comment_0_0.csv --nodes forum_0_0.csv --nodes person_0_0.csv --nodes post_0_0.csv --relationships comment_hasCreator_person_0_0.csv --relationships comment_hasTag_tag_0_0.csv --relationships comment_isLocatedIn_place_0_0.csv --relationships comment_replyOf_comment_0_0.csv --relationships comment_replyOf_post_0_0.csv --relationships forum_containerOf_post_0_0.csv --relationships forum_hasMember_person_0_0.csv --relationships forum_hasModerator_person_0_0.csv --relationships forum_hasTag_tag_0_0.csv --relationships person_hasInterest_tag_0_0.csv --relationships person_isLocatedIn_place_0_0.csv --relationships person_knows_person_0_0.csv --relationships person_likes_comment_0_0.csv --relationships person_likes_post_0_0.csv --relationships person_studyAt_organisation_0_0.csv --relationships person_workAt_organisation_0_0.csv --relationships post_hasCreator_person_0_0.csv --relationships post_hasTag_tag_0_0.csv --relationships post_isLocatedIn_place_0_0.csv --delimiter "|" --array-delimiter ";"




