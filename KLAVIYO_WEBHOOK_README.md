# Homefans Klaviyo Recommendation Webhook

This keeps `upsell_recommendations_wide.csv` as the recommendation source of truth and adds the missing automation layer:

`order webhook -> product lookup -> Klaviyo event -> post-purchase upsell flow`

## What The Script Sends

For each eligible order, `klaviyo_order_recommendation_webhook.py` sends this custom Klaviyo event:

`Placed Order Recommendation Ready`

The event includes the fields needed to render the email:

- `email`
- `purchased_product`
- `recommended_1_title`
- `recommended_1_url`
- `recommended_2_title`
- `recommended_2_url`
- `recommended_3_title`
- `recommended_3_url`

It also includes helpful QA fields such as `matched_product`, `match_score`, `recommended_1_basis`, `recommended_1_location_proximity`, `recommended_1_confidence`, `recommended_1_phi`, and `recommended_1_lift`.

Mapped products include:

```text
recommendation_mode=mapped
```

Unmapped, incomplete, missing, or generic product titles still send the same event with:

```text
recommendation_mode=generic
```

The generic fallback pulls three high-volume, recent, active Homefans products from `upsell_recommendations_wide.csv`, keeps the recommendations geographically diverse where possible, and adds the same Klaviyo UTM structure.

## Environment Variables

Required for live sending:

```bash
export KLAVIYO_PRIVATE_API_KEY="pk_..."
```

Recommended:

```bash
export WEBHOOK_SECRET="choose-a-long-random-secret"
export RECOMMENDATIONS_CSV="/Users/ronanliedmeier/Documents/Klaviyo/upsell_recommendations_wide.csv"
export KLAVIYO_EVENT_NAME="Placed Order Recommendation Ready"
export KLAVIYO_REVISION="2024-07-15"
export MIN_RECOMMENDATIONS="3"
export GENERIC_FALLBACK="true"
```

`MIN_RECOMMENDATIONS=3` means the script only fires the event when all three product cards are available. Use `MIN_RECOMMENDATIONS=1` only if the Klaviyo template can safely hide empty recommendation slots.

`GENERIC_FALLBACK=true` means unmapped products still trigger the Klaviyo event with generic recommendations. Use `--no-generic-fallback` if you need the old skip behavior for a test run.

## Local Dry Run

```bash
python3 /Users/ronanliedmeier/Documents/Klaviyo/klaviyo_order_recommendation_webhook.py \
  --dry-run \
  --email test@example.com \
  --product-title "Watch Flamengo at Maracana: Ticket + Matchday Experience with a Local!"
```

WooCommerce-like payload test:

```bash
printf '%s\n' '{"id":12345,"billing":{"email":"test@example.com"},"line_items":[{"name":"Watch Flamengo at Maracana: Ticket + Matchday Experience with a Local!"}]}' \
  | python3 /Users/ronanliedmeier/Documents/Klaviyo/klaviyo_order_recommendation_webhook.py --dry-run --stdin
```

Unmapped product generic fallback test:

```bash
python3 /Users/ronanliedmeier/Documents/Klaviyo/klaviyo_order_recommendation_webhook.py \
  --dry-run \
  --email test@example.com \
  --product-title "Unmapped Homefans Experience Test Product"
```

## Run As A Webhook Receiver

```bash
DRY_RUN=true python3 /Users/ronanliedmeier/Documents/Klaviyo/klaviyo_order_recommendation_webhook.py --serve --port 8080
```

POST order webhooks to:

```text
https://your-deployed-domain.com/webhook
```

If `WEBHOOK_SECRET` is set, include this HTTP header in the webhook sender:

```text
X-Webhook-Secret: choose-a-long-random-secret
```

## Klaviyo Flow Setup

Create a flow triggered by the metric:

`Placed Order Recommendation Ready`

In the email template, use the custom event properties:

```text
{{ event.recommended_1_title }}
{{ event.recommended_1_url }}
{{ event.recommended_2_title }}
{{ event.recommended_2_url }}
{{ event.recommended_3_title }}
{{ event.recommended_3_url }}
```

If you want separate copy for specific versus generic emails, branch on:

```text
{{ event.recommendation_mode }}
```

Use `mapped` for product-specific upsells and `generic` for the fallback/bestseller-style email.

The recommendation URLs already include:

```text
utm_source=klaviyo
utm_medium=email
utm_campaign=post_purchase_upsell
utm_content=upsell_suggestion_1/2/3
```

## API Notes

The script uses Klaviyo's server-side Events API:

- Endpoint: `POST https://a.klaviyo.com/api/events`
- Header: `Authorization: Klaviyo-API-Key <private_key>`
- Required key scope: `events:write`
- Content type: `application/vnd.api+json`

Official docs:

- https://developers.klaviyo.com/en/reference/events_api_overview
- https://developers.klaviyo.com/en/reference/create_event
- https://developers.klaviyo.com/en/v2024-07-15/reference/api_overview
