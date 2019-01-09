----------------
--it might be confusing, as this table looks like it could just be executed via the 'uc' table, however, this enables us to 
--deal with situations where a campaign is turned on and backfilled midway through a month (see the UNION ALL portion of query)
--for ensuring a complete dataset of cookies for a month
----------------
WITH uc_base_stitch AS (
SELECT 
	_partitiontime as bkt, 
	 bk_campaign_id, 
	 partner_id 
FROM `nyt-adtech-prd.bluekai_data.user_campaigns` 
WHERE 
	DATE(_partitiontime) BETWEEN date_sub(DATE('2018-12-01'), interval 30 day) AND date_sub(DATE('2019-01-01'), interval 1 day)--change these dates; applies 30 day lookback for cookies
	AND bk_campaign_id IS NOT NULL --we don't want to pull in data for campaigns that are null (truly just a cleaning step)
	AND bk_campaign_id IN 
		(SELECT campaign_id 
		FROM `nyt-bigquery-beta-workspace.palmer_data.whitelisted_readerscope`) --this is the whitelisted campaign list for readerscope, since some campaigns also exist for
																				--marketing purposes. We want to continuously update this, as needed, which should be dictated
																				--by Nicholas Eckhart and/or the data product manager on his team. We can consider making this a
																				--Google Sheet that auto updates a table in BQ if we would like to make this process easier
																				--(i.e. everytime Nick makes an update in his Google Sheet, it auto populates BQ)
	AND bk_campaign_id IN --ensures that we aren't bringing anything in anything that doesn't have a campaign name
		(SELECT campaign_id 
		FROM `nyt-adtech-prd.bluekai_data.campaign_metadata`) 
GROUP BY 1,2,3 
/*UNION ALL --as you can see, this is commented out as we ONLY need to include UNION(s) when we have to stitch in campaigns that were turned on mid-month
			--each unique campaign_id necessitates a new UNION ALL statement, please keep that in mind
 		SELECT 
 			TIMESTAMP("2018-11-30") as bkt, -- this is an example date. This should be the date for the day PRIOR to the start of the month you care about
 											--for example, if my audience was turned on mid December and I want to supply our Data Scientist with December bluekai data
 											--then I would do the (MIN(date of month we care about)-1), which is 11/30 in this case, here. See bottom of code for 
 											--more in-depth understanding as to why.
 			bk_campaign_id, 
 			partner_id
 		FROM `nyt-adtech-prd.bluekai_data.user_campaigns` uc
 		WHERE _partitiontime BETWEEN TIMESTAMP('2018-12-05') AND TIMESTAMP('2018-12-07') --this is +1 and -1 day from the date that the campaign was turned on. 
 																						 --in this case, the campaign was turned on 12/6, so we do btw 12/5 and 12/7
 		AND bk_campaign_id IS NOT NULL --similar to before, we don't want a campaign_id to come in that isn't null (for efficiency sake)
 		AND bk_campaign_id=330275  --this is is the campaign_id of the bluekai campaign that was turned on mid-month that we care about
 */
),

----------------
--this table is used to ensure we have all of the data stitched together that we need. Of course, if we have no campaigns 
--that are turned on mid-month, this will seem redundant. But keeping this here keeps the overall query more templatized.
--this is all of the bluekai data that we need.
----------------
uc AS (
SELECT
	bkt,
	partner_id,
	bk_campaign_id
FROM uc_base_stitch
),

----------------
--now we have moved on from bluekai and are into et and content. This helps us know, what campaigns did these audiences read about?
--the first subquery for et is going to give us the ability to join our `uc` bluekai data to pubp's assets and also provide
--dma and geo-related information. As you can see, the join key is agent_id below.
----------------
et AS(
SELECT 
	agent_id, 
	geo.dma_code AS dma_code, 
	geo.country AS country, 
 	safe_CAST(asset.id AS STRING) AS asset_id, --to ensure our datatypes are the same for when we join et to pubp
 	datum_id, 
 	_partitiontime AS dt 
FROM `nytdata.et.page`  
WHERE _partitiontime BETWEEN '2018-12-01' AND '2018-12-31' --this is the date range of the full month we care about. 
														   --AKA, "What did audiences read in the month Dec?"
														   --December dates would go here.
	AND asset.id IS NOT NULL  --this filters out hp/sf/interactives/slideshows as agreed upon as they do not have assetIds. 
							  --this has been previously agreed on, but can be updated in the future as needed.
	AND geo.country IS NOT NULL --because knowing where our cookies come from is important to the RS product, we only include non-null geo values here
),

----------------
--now we have our et data, let's get the actual uri's from pubp
----------------
pubp AS(
SELECT 
	publish.article.publicationProperties.uri AS uri,
	publish.article.publicationProperties.sourceId AS sourceId
FROM `nyt-pubp-prd.origin_prd.latest_versions_of_assets`
WHERE publish.article.publicationProperties.uri IS NOT NULL 
),

----------------
--this is our join CTE between et and pubp, which will, in the end, allow us to join pubp uri's, et locations and bluekai audiences together
----------------
et_pubp AS(
SELECT
	pubp.uri, 
	et.agent_id, 
	et.dma_code, 
	et.country, 
	et.asset_id, 
	et.datum_id, 
	et.dt 
FROM et 
	JOIN pubp --note this is an inner join, there must be a match between et and pubp for us to pull in those uri's
		ON et.asset_id = pubp.sourceId
)

----------------
--here is our final table, the most confusing part here is the 'where clause', which is covered below
----------------
SELECT 
	et_pubp.uri AS uri, 
	et_pubp.dma_code, 
	et_pubp.country, 
	uc.bk_campaign_id AS campaign_id, 
	et_pubp.datum_id, 
 	et_pubp.dt 
 FROM uc
	 JOIN et_pubp --again, an inner join as we only want et_pubp data when they have a match to bluekai data
	 	ON et_pubp.agent_id = uc.partner_id
 WHERE bkt < et_pubp.dt AND bkt >= timestamp_sub(et_pubp.dt, interval 30 day)  --what's going on here, right?
 																			   --we have decided that we only want to start following a bluekai campaign's et/pubp behavior
 																			   --the day after it has been registered within bluekai. this allows us to be certain we are viewing
 																			   --cookie behavior on its first full day being a part of a campaign.
 GROUP BY 1,2,3,4,5,6

