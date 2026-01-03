# Organization Sponsorship Support

## Overview

The application now supports organization-level sponsorships where organizations that sponsor at **$100+/month** get shared credit pools that all organization members can access. This prevents credit duplication and provides a centralized credit management system for teams.

## Key Features

### 1. Organization Credit Pools
- **Minimum Tier**: $100/month = 500M shared tokens
- Credits are shared among all organization members
- No duplication of credits per user

### 2. Credit Priority System
- **Personal credits used first**: When a user makes an API request, their personal sponsorship credits are checked first
- **Organization credits as fallback**: If personal credits are exhausted, the system automatically checks all organizations the user belongs to
- **Transparent switching**: Users don't need to specify which credit pool to use - it's handled automatically

### 3. Automatic Membership Sync
Organization memberships are synced automatically:
- When user logs in via GitHub OAuth
- When user manually refreshes their sponsorship status
- System queries GitHub GraphQL API for organization memberships
- Creates/updates Organizations table with sponsorship details
- Manages OrganizationMember junction records
- Removes stale memberships when users leave organizations or organizations stop sponsoring

## Database Schema

### Organizations Table
```sql
CREATE TABLE Organizations (
    Id INT PRIMARY KEY IDENTITY,
    GitHubLogin NVARCHAR(450) NOT NULL UNIQUE,
    AvatarUrl NVARCHAR(MAX),
    TokensAllowedPerMonth INT NOT NULL,
    CreatedAt DATETIME2 NOT NULL,
    LastUpdatedAt DATETIME2 NOT NULL
)
```

### OrganizationMembers Junction Table
```sql
CREATE TABLE OrganizationMembers (
    Id INT PRIMARY KEY IDENTITY,
    UserId INT NOT NULL,
    OrganizationId INT NOT NULL,
    CreatedAt DATETIME2 NOT NULL,
    CONSTRAINT FK_OrganizationMembers_Users FOREIGN KEY (UserId) REFERENCES Users(Id) ON DELETE CASCADE,
    CONSTRAINT FK_OrganizationMembers_Organizations FOREIGN KEY (OrganizationId) REFERENCES Organizations(Id) ON DELETE CASCADE,
    CONSTRAINT UQ_OrganizationMembers_User_Organization UNIQUE (UserId, OrganizationId)
)
```

### Updated TokenUsages Table
```sql
ALTER TABLE TokenUsages ADD OrganizationId INT NULL
ALTER TABLE TokenUsages ALTER COLUMN UserId INT NULL

-- Both UserId and OrganizationId can be NULL, but one must be set
-- Indexes on both (UserId, UsageDate) and (OrganizationId, UsageDate)
```

## API Behavior

### Credit Checking Flow
1. User makes request with their API key
2. System validates API key and loads user with organization memberships
3. Checks user's personal credit balance for current month
4. If insufficient, iterates through user's organizations to find one with available credits
5. Selected credit pool (user or org) is recorded for token usage tracking

### Token Recording
- When using personal credits: Record against `UserId` in TokenUsages
- When using org credits: Record against `OrganizationId` in TokenUsages
- Each record tracks which credit pool was consumed
- Race-condition-safe atomic updates using `ExecuteUpdate`

## Dashboard Display

### Personal Section
Shows user's personal sponsorship:
- Sponsorship tier level
- Personal credits available
- Personal credits used this month
- Personal monthly allowance
- API key with copy button

### Organization Section
Dynamically displays all organizations:
- Organization avatar and name
- Shared pool available
- Shared pool used this month
- Shared pool monthly allowance
- Pink sponsor gradient styling to distinguish from personal credits

## Implementation Details

### GitHubSponsorshipService
New methods:
- `GetAllSponsorships(accessToken)`: Returns list of all sponsorships (users and orgs) with avatar URLs
- `GetOrganizationMemberships(accessToken)`: Queries GitHub for user's organization memberships
- `MapTierToTokens(cents, isOrganization)`: Maps tier amounts to tokens, enforces $100 minimum for orgs

### HomeController
New method:
- `UpdateOrganizationMemberships(username, accessToken)`: Syncs organization sponsorships and memberships
  - Fetches all sponsorships including organizations
  - Filters to organizations where user is a member
  - Creates/updates Organizations table
  - Manages OrganizationMember junction records
  - Removes stale memberships

Integration points:
- Called in `GitHubCallback` action after user creation
- Called in `RefreshSponsorship` action to keep memberships current

### AIController
Enhanced credit checking:
- Loads user with `Include(u => u.OrganizationMemberships).ThenInclude(om => om.Organization)`
- Checks personal credits first
- Falls back to organization credits if personal exhausted
- Stores selected organization ID in `HttpContext.Items` for token recording
- Updated `RecordTokenUsage` to accept optional `organizationId` parameter
- Records usage against appropriate credit pool (user or organization)

## Testing Recommendations

1. **Create test organization sponsorship** at $100/month
2. **Verify organization appears** in database after login
3. **Confirm multiple users** in same org see shared pool
4. **Test credit consumption** from org pool when personal credits exhausted
5. **Verify membership removal** when user leaves org or org stops sponsoring
6. **Check dashboard display** shows all linked organizations correctly

## Migration History

- `20260103183340_AddOrganizationsSupport`: Complete schema for organizations, memberships, and updated token usage tracking

## Security Considerations

- Organization memberships verified via GitHub GraphQL API
- Users can only access credits from organizations they belong to
- Cascade delete ensures orphaned records are cleaned up when users/orgs are removed
- API key validation remains unchanged - authentication is per-user, not per-organization
