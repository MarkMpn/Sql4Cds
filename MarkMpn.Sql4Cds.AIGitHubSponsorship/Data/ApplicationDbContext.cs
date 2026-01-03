using MarkMpn.Sql4Cds.AIGitHubSponsorship.Models;
using Microsoft.EntityFrameworkCore;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        public DbSet<User> Users { get; set; }
        public DbSet<TokenUsage> TokenUsages { get; set; }
        public DbSet<Organization> Organizations { get; set; }
        public DbSet<OrganizationMember> OrganizationMembers { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Configure User entity
            modelBuilder.Entity<User>(entity =>
            {
                entity.HasIndex(e => e.GitHubUsername).IsUnique();
                entity.HasIndex(e => e.ApiKey).IsUnique();
                
                entity.Property(e => e.CreatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");
                
                entity.Property(e => e.LastUpdatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");
            });

            // Configure TokenUsage entity
            modelBuilder.Entity<TokenUsage>(entity =>
            {
                entity.HasIndex(e => new { e.UserId, e.UsageDate }).IsUnique(false);
                entity.HasIndex(e => new { e.OrganizationId, e.UsageDate }).IsUnique(false);
                
                entity.Property(e => e.CreatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");
                
                entity.Property(e => e.LastUpdatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");

                entity.HasOne(e => e.User)
                    .WithMany(u => u.TokenUsages)
                    .HasForeignKey(e => e.UserId)
                    .OnDelete(DeleteBehavior.Cascade);

                entity.HasOne(e => e.Organization)
                    .WithMany(o => o.TokenUsages)
                    .HasForeignKey(e => e.OrganizationId)
                    .OnDelete(DeleteBehavior.Cascade);
            });

            // Configure Organization entity
            modelBuilder.Entity<Organization>(entity =>
            {
                entity.HasIndex(e => e.GitHubLogin).IsUnique();
                
                entity.Property(e => e.CreatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");
                
                entity.Property(e => e.LastUpdatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");
            });

            // Configure OrganizationMember entity
            modelBuilder.Entity<OrganizationMember>(entity =>
            {
                entity.HasIndex(e => new { e.UserId, e.OrganizationId }).IsUnique();
                
                entity.Property(e => e.CreatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");

                entity.HasOne(e => e.User)
                    .WithMany(u => u.OrganizationMemberships)
                    .HasForeignKey(e => e.UserId)
                    .OnDelete(DeleteBehavior.Cascade);

                entity.HasOne(e => e.Organization)
                    .WithMany(o => o.Members)
                    .HasForeignKey(e => e.OrganizationId)
                    .OnDelete(DeleteBehavior.Cascade);
            });
        }
    }
}
