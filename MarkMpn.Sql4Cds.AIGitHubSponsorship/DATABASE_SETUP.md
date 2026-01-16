# Database Setup Instructions

## Prerequisites
- SQL Server or SQL Server LocalDB installed
- .NET 8.0 SDK

## Steps to Create Database

### 1. Restore NuGet Packages
```powershell
dotnet restore
```

### 2. Create Initial Migration
```powershell
dotnet ef migrations add InitialCreate
```

### 3. Update Database
```powershell
dotnet ef database update
```

## Database Schema

### Users Table
- **Id** (int, PK) - Auto-incrementing user identifier
- **GitHubUsername** (nvarchar(100), unique, required) - GitHub username
- **AccessToken** (nvarchar(500), required) - GitHub OAuth access token
- **TokensAllowedPerMonth** (int) - Number of AI tokens allocated per month
- **CreatedAt** (datetime) - User creation timestamp
- **LastUpdatedAt** (datetime) - Last update timestamp

### TokenUsages Table
- **Id** (int, PK) - Auto-incrementing record identifier
- **UserId** (int, FK to Users) - Reference to user
- **UsageDate** (date) - Date of token usage
- **TokensUsed** (int) - Number of tokens consumed on this date
- **CreatedAt** (datetime) - Record creation timestamp
- **LastUpdatedAt** (datetime) - Last update timestamp

## Key Features
- Unique constraint on GitHubUsername in Users table
- Composite unique index on (UserId, UsageDate) in TokenUsages table
- Cascade delete: Deleting a user removes all their token usage records
- Default values set for CreatedAt and LastUpdatedAt using SQL Server GETUTCDATE()

## Connection String
The default connection string in `appsettings.json` uses LocalDB:
```
Server=(localdb)\\mssqllocaldb;Database=Sql4CdsAISponsorship;Trusted_Connection=True;MultipleActiveResultSets=true
```

Modify this in `appsettings.json` or `appsettings.Development.json` to match your SQL Server configuration.

## Troubleshooting

### If migrations fail
Make sure you're in the project directory and have EF Core tools installed:
```powershell
dotnet tool install --global dotnet-ef
```

### To remove last migration (if needed)
```powershell
dotnet ef migrations remove
```

### To view SQL that will be executed
```powershell
dotnet ef migrations script
```
