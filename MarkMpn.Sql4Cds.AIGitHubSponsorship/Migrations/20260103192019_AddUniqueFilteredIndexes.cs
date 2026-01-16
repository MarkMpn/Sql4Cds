using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Migrations
{
    /// <inheritdoc />
    public partial class AddUniqueFilteredIndexes : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_TokenUsages_OrganizationId_UsageDate",
                table: "TokenUsages");

            migrationBuilder.DropIndex(
                name: "IX_TokenUsages_UserId_UsageDate",
                table: "TokenUsages");

            migrationBuilder.CreateIndex(
                name: "IX_TokenUsages_OrganizationId_UsageDate",
                table: "TokenUsages",
                columns: new[] { "OrganizationId", "UsageDate" },
                unique: true,
                filter: "[OrganizationId] IS NOT NULL");

            migrationBuilder.CreateIndex(
                name: "IX_TokenUsages_UserId_UsageDate",
                table: "TokenUsages",
                columns: new[] { "UserId", "UsageDate" },
                unique: true,
                filter: "[UserId] IS NOT NULL");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_TokenUsages_OrganizationId_UsageDate",
                table: "TokenUsages");

            migrationBuilder.DropIndex(
                name: "IX_TokenUsages_UserId_UsageDate",
                table: "TokenUsages");

            migrationBuilder.CreateIndex(
                name: "IX_TokenUsages_OrganizationId_UsageDate",
                table: "TokenUsages",
                columns: new[] { "OrganizationId", "UsageDate" });

            migrationBuilder.CreateIndex(
                name: "IX_TokenUsages_UserId_UsageDate",
                table: "TokenUsages",
                columns: new[] { "UserId", "UsageDate" });
        }
    }
}
