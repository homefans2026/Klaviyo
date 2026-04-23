# Homefans Klaviyo Upsell Analysis

Generated using live Homefans snapshot 2026-04-14 from:
- `/Users/ronanliedmeier/Downloads/wc_order-export-2026-04-13.csv`

## Usable data
- Orders used: 4814
- Customer identities used: 4128
- Distinct parsed products/items: 269
- Product coverage window in this export: 2025-05-26 to 2026-04-13
- Suggestion activity window enforced: 2025-04-13 to 2026-04-13
- Products eligible to appear as suggestions: 269
- Live Homefans catalog snapshot used for URL matching: 497 published products
- Homefans location taxonomy snapshot used for proximity scoring: 966 location terms
- Generic product explicitly excluded from all recommendations: Ticket Sale

## Method
1. Kept only `Completed` and `Booking confirmed` orders with a populated `Products` field.
2. Used email as the customer key for normal customers, but mapped `@homefans.net` and `@homefans.com` orders to the guest name so manual bookings did not collapse into operational inboxes.
3. Parsed the `Products` cell into individual items by comma to capture matchday products plus common upgrades/add-ons.
4. Calculated three signals for every product pair:
   - `later_customer_count`: customers who bought the suggested product on a later order after first buying the base product.
   - `same_order_count`: orders where both items appeared together.
   - `phi_coefficient`: customer-level binary correlation between the two products across the usable dataset.
5. Added a recency multiplier inside the last-12-month window so the most recent orders contribute more than older orders when ranking candidate upsells.
6. Allowed a product to appear as a suggested upsell only if it had at least one sale inside the last-12-month activity window above.
7. Removed `Ticket Sale` from both base-product and suggested-product consideration because it is a generic placeholder rather than a meaningful upsell.
8. Matched suggested products against the live Homefans catalog snapshot with stronger token-level checks so generic words like `city`, `matchday`, and `experience` cannot create false URL matches on their own.
9. Used the Homefans location taxonomy both for live-product validation and for fallback title-based location inference when a product could not be matched confidently to a live URL.
10. Used the Homefans location taxonomy to prioritize geographic proximity in this order: same city, same country, ancillary add-ons, same continent, then broader fallbacks only when closer options were not available.
11. Applied a commercial QA filter that keeps same-city, same-country, true ancillary bundles, and only unusually strong broader regional recommendations. Weak same-continent and unknown-location guesses are not exported.
12. Ranked recommendations inside each proximity bucket by prioritizing strong same-order bundles first, then positively correlated later purchases, then recent activity and freshness inside the last 12 months.
13. Added Klaviyo email UTM tracking to exported suggestion URLs: `utm_source=klaviyo`, `utm_medium=email`, `utm_campaign=post_purchase_upsell`, and rank-specific `utm_content`.

## Output files
- `upsell_recommendations_wide.csv`: one row per base product, with up to 3 suggested products.
- `upsell_recommendations_detailed.csv`: one row per base product / suggestion pair with all supporting metrics.
- `homefans_link_audit_2026-04-20.csv`: live HTTP validation for every exported product URL.
- `homefans_live_product_catalog_2026-04-14.json`: live catalog snapshot used for URL enrichment.
- `homefans_location_taxonomy_2026-04-14.json`: taxonomy snapshot used for location proximity scoring.

## Coverage note
- 61 products have 3 data-backed suggestions.
- 207 products have fewer than 3 recommendations in this export and are flagged with `needs_manual_fill=yes`.
- 357 exported URL entries were checked live on 2026-04-20; all 357 returned working 2xx/3xx responses.

## Interpretation notes
- Positive `phi_coefficient` means the products are positively associated across customers.
- Negative `phi_coefficient` can still appear when a recommendation is supported by later-purchase or same-order behavior but the overall customer overlap is small.
- Because historical orders before June 2025 mostly do not contain product names in this export, the recommendations are effectively driven by June 2025 to April 13, 2026 product-level behavior.
- Suggested products in the exported sheet now also carry their matched live Homefans URL, matched live title, a `location_proximity` label, a `commercial_review` label, and a `recency_weighted_score` that reflects both behavioral fit and recent sales momentum.
- `suggestion_*_url` is the Klaviyo-ready UTM-tagged URL. `suggestion_*_url_clean` preserves the original Homefans product URL used during live-link validation.
- Repeat-purchase behavior is sparse for many flagship experiences, so immediate post-purchase Klaviyo flows will likely perform best with `same_order_bundle` suggestions and only secondarily with `later_lifecycle` suggestions.
