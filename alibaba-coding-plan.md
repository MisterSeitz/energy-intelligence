Coding Plan is a subscription plan from Alibaba Cloud Model Studio. It charges a fixed monthly fee and provides a monthly request quota to use qwen3-coder-plus in AI coding tools such as Qwen Code, Claude Code, and Cline.

Important
Coding Plan supports only the Singapore region and the global deployment mode. Model inference runs globally, and your inputs and outputs will be transferred across borders. Ensure your use complies with applicable laws and regulations.

Advantages
Latest coder model: Coding Plan supports qwen3-coder-plus, Qwen's latest coder model. It has powerful coding agent capabilities, excels at tool calling and environment interaction, and provides excellent coding performance. It also offers general features such as dialogue, translation, and summarization.

Cost-effective subscription: Coding Plan charges a fixed monthly fee. Up to 90,000 requests per month meets frequent demands at a predictable cost.

Compatible with tools: Coding Plan supports AI coding tools that are compatible with the OpenAI API and Anthropic API specifications, such as Qwen Code, Claude Code, and Cline.

Overview
Limited-time offer
From 00:00:00 (UTC+8) on January 16, 2026, to 23:59:59 (UTC+8) on March 31, 2026, new Alibaba Cloud users who have linked a payment card can claim a coupon for a first-month discount on the promotion page. This offer excludes accounts with enterprise discounts, virtual operators, and RAM users.

50% off coupon: After you claim the coupon, receive 50% off both the Lite and Pro plans, up to $100.

Note
The offer is available only to new Alibaba Cloud users, who have never placed a paid order for more than $0 on Alibaba Cloud.

Each eligible account can claim a coupon only once.

It cannot be combined with other coupons and discounts.

Pricing and usage
A single user question may trigger multiple model calls, with each counted as one request.




Type

Lite

Pro

Price

$10/month

$50/month

Limit per 5 hours

Up to 1,200 requests

Up to 6,000 requests

Weekly limit

Up to 9,000 requests

Up to 45,000 requests

Monthly limit

Up to 18,000 requests

Up to 90,000 requests

Model

qwen3-coder-plus

qwen3-coder-plus

Refresh rules
Limit per 5 hours: This is a dynamic limit, restored 5 hours after a request is consumed.

Weekly limit: Resets every Monday at 00:00:00 (UTC+8).

Monthly limit: Resets on the corresponding day of each subscription month at 00:00:00 (UTC+8).

Limitations
Applicable tools: A single plan can be used across all supported coding tools, such as Claude Code and Qwen Code, with a shared quota.


Account policy: The plan is for the exclusive use of the subscriber and must not be shared. Account sharing may lead to restrictions on subscription benefits.

Quick integration
Subscribe to a coding plan
Important
Before subscribing to Coding Plan, note the following important terms:

Only Alibaba Cloud accounts can subscribe to Coding Plan. RAM users are not supported.

Coding Plan subscriptions are non-cancellable and non-refundable once purchased.

Go to the Model Studio Coding Plan subscription page and purchase a plan that meets your requirements.

Go to the Coding Plan console to get your plan-specific API key.

The plan-specific API key is in the format sk-sp-xxxxx. It is different from the general-purpose Model Studio API key. Do not use them interchangeably.
Connect to tools
Qwen CodeClaude CodeClineOther tools
Install Qwen Code.

 
npm install -g @qwen-code/qwen-code@latest
Configure environment variables.

macOSWindows
Check your default shell type.

 
echo $SHELL
Set the environment variables based on your shell type:

ZshBash
 
# Replace YOUR_API_KEY with your plan-specific API key
echo 'export OPENAI_API_KEY="YOUR_API_KEY"' >> ~/.zshrc
echo 'export OPENAI_BASE_URL="https://coding-intl.dashscope.aliyuncs.com/v1"' >> ~/.zshrc
echo 'export OPENAI_MODEL="qwen3-coder-plus"' >> ~/.zshrc
Apply the environment variables.

ZshBash
 
source ~/.zshrc
Connect to Qwen Code.

Open your terminal and navigate to your project directory. Start Qwen Code.

 
cd path/to/your_project
qwen
After Qwen Code starts, select OpenAI for identity authentication and confirm the environment variable configuration.

image

If the preceding interface does not appear, enter /auth to set the identity authentication method.
Use qwen3-coder-plus in Qwen Code.

image

Subscription management
View subscription usage, renew plans, and upgrade plans in the Coding Plan console.

Auto-renewal
When you subscribe to a coding plan, select Auto-renewal to prevent interruption due to expiration. The auto-renewal cycle is monthly. Your payment method is automatically charged at the real-time price before the instance expires. Cancel auto-renewal at any time on the Renewals page.

Cancellations and refunds
Coding Plan subscriptions are non-cancellable and non-refundable once purchased. Choose the plan and term that suits your needs.

Upgrades and downgrades
Upgrade from the Lite plan to the Pro plan: The upgrade fee is the price difference prorated for the remaining days. After the upgrade, the available quota for the remainder of the month is increased proportionally based on the original remaining quota.

Cannot downgrade from the Pro plan to the Lite plan. Wait for your Pro plan to expire and then purchase a new Lite plan.

FAQ
Common errors and solutions



Error message

Possible cause

Solution

401 invalid access token or token expired

The API key is invalid, expired, empty, in the wrong format, or does not match the endpoint environment.

Check that the API key is the plan-specific key for Coding Plan. Copy the full key without any spaces. Confirm that your subscription is active.

model 'xxx' is not supported

The model name is misspelled, has incorrect capitalization, contains spaces, or is an unsupported model.

The model name must be qwen3-coder-plus. It is case-sensitive and must not have leading or trailing spaces.

403 invalid api-key

The general-purpose Model Studio base URL was used by mistake.

Anthropic-compatible endpoint: https://coding-intl.dashscope.aliyuncs.com/apps/anthropic

OpenAI-compatible endpoint: https://coding-intl.dashscope.aliyuncs.com/v1

404 status code (no body)

Incorrect base URL path.

Anthropic-compatible endpoint: https://coding-intl.dashscope.aliyuncs.com/apps/anthropic

OpenAI-compatible endpoint: https://coding-intl.dashscope.aliyuncs.com/v1

Connection error

The base URL is misspelled or there is a network issue.

Check the spelling of the base URL domain name and your network connectivity.

hour allocated quota exceeded

The request quota per 5 hours has been used up.

Wait 5 hours for the quota to be automatically restored, or upgrade to Pro.

week allocated quota exceeded

The weekly request quota has been used up.

Wait until the quota resets at 00:00:00 (UTC+8) on Monday, or upgrade to Pro.

month allocated quota exceeded

The monthly request quota has been used up.

Wait until the quota resets at 00:00:00 (UTC+8) on the corresponding day of your subscription month, or upgrade to Pro.

Can I use models other than qwen3-coder-plus?
No. Coding Plan supports only qwen3-coder-plus.

How many Coding Plans can each account have at the same time?
Each Model Studio account can have only one active Coding Plan subscription at a time, regardless of whether it is Lite or Pro.

Can I use the Model Studio free trial with Coding Plan?
No. Coding Plan is a standalone subscription product, separated from the free quota of Model Studio.