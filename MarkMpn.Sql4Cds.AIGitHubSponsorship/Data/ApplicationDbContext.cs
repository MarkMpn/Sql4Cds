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
        public DbSet<ManualCreditAssignment> ManualCreditAssignments { get; set; }

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
                // Unique index on UserId + UsageDate where UserId is not null
                entity.HasIndex(e => new { e.UserId, e.UsageDate })
                    .IsUnique()
                    .HasFilter("[UserId] IS NOT NULL");
                
                // Unique index on OrganizationId + UsageDate where OrganizationId is not null
                entity.HasIndex(e => new { e.OrganizationId, e.UsageDate })
                    .IsUnique()
                    .HasFilter("[OrganizationId] IS NOT NULL");
                
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

            // Configure ManualCreditAssignment entity
            modelBuilder.Entity<ManualCreditAssignment>(entity =>
            {
                // One manual assignment per user (optional)
                entity.HasIndex(e => e.UserId)
                    .IsUnique()
                    .HasFilter("[UserId] IS NOT NULL");

                // One manual assignment per organization (optional)
                entity.HasIndex(e => e.OrganizationId)
                    .IsUnique()
                    .HasFilter("[OrganizationId] IS NOT NULL");

                entity.Property(e => e.CreatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");

                entity.Property(e => e.LastUpdatedAt)
                    .HasDefaultValueSql("GETUTCDATE()");

                entity.HasOne(e => e.User)
                    .WithMany()
                    .HasForeignKey(e => e.UserId)
                    .OnDelete(DeleteBehavior.Cascade);

                entity.HasOne(e => e.Organization)
                    .WithMany()
                    .HasForeignKey(e => e.OrganizationId)
                    .OnDelete(DeleteBehavior.Cascade);
            });
        }
    }
}
