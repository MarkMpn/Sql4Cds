using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Migrations
{
    /// <inheritdoc />
    public partial class AddApiKeyToUser : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "ApiKey",
                table: "Users",
                type: "nvarchar(200)",
                maxLength: 200,
                nullable: false,
                defaultValue: "");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "ApiKey",
                table: "Users");
        }
    }
}
