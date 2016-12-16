using System;
using System.Data.Entity;
using System.Linq;
using EntityFramework.BulkExtensions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EntityFrameworkBulkExtensions.Tests
{
    [TestClass]
    public class EntityFrameworkBulkExtensionsTests
    {
        [TestMethod]
        public void GraphWithoutInheritanceOrLoops_Works()
        {
            var context = new FooContext();

            var a = new A();
            var b = new B();
            var c = new C();
            var d = new D() { A = a, B = b, C = c };
            var e = new E() { B = b };
            var f = new F() { D = d, E = e };
            var g = new G() { D = d };
            var h = new H() { C = c, E = e };

            context.As.Add(a);
            context.Bs.Add(b);
            context.Cs.Add(c);
            context.Ds.Add(d);
            context.Es.Add(e);
            context.Fs.Add(f);
            context.Gs.Add(g);
            context.Hs.Add(h);

            context.BulkSaveAdditions();
            
            Assert.IsTrue(EntityExistsInDatabase(a));
            Assert.IsTrue(EntityExistsInDatabase(b));
            Assert.IsTrue(EntityExistsInDatabase(c));
            Assert.IsTrue(EntityExistsInDatabase(d));
            Assert.IsTrue(EntityExistsInDatabase(e));
            Assert.IsTrue(EntityExistsInDatabase(f));
            Assert.IsTrue(EntityExistsInDatabase(g));
            Assert.IsTrue(EntityExistsInDatabase(h));
        }

        [TestMethod]
        public void BulkSaveAdditions_ResetsChanges_SoConsecutiveCallDoesNothing()
        {
            var context = new FooContext();

            context.As.Add(new A());
            context.SaveChanges();

            var items = Enumerable.Range(0, 1000).Select(x => new A()).ToList();
            context.As.AddRange(items);
            context.BulkSaveAdditions();
            Assert.IsTrue(EntityExistsInDatabase(items.First()));
            Assert.IsTrue(EntityExistsInDatabase(items.Last()));

            context.BulkSaveAdditions();
            Assert.IsTrue(EntityExistsInDatabase(items.First()));
            Assert.IsTrue(EntityExistsInDatabase(items.Last()));

            context.As.Add(new A());
            context.SaveChanges();
        }

        private bool EntityExistsInDatabase<T>(T t) where T : Entity
        {
            using (var context = new FooContext())
            {
                return context.Set<T>().Single(x => x.Id == t.Id) != null;
            }
        }
    }

    class FooContext : DbContext
    {
        public DbSet<A> As { get; set; }
        public DbSet<B> Bs { get; set; }
        public DbSet<C> Cs { get; set; }
        public DbSet<D> Ds { get; set; }
        public DbSet<E> Es { get; set; }
        public DbSet<F> Fs { get; set; }
        public DbSet<G> Gs { get; set; }
        public DbSet<H> Hs { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Entity<D>().HasRequired(d => d.A).WithMany().HasForeignKey(d => d.A_Id).WillCascadeOnDelete(false);
            modelBuilder.Entity<D>().HasRequired(d => d.B).WithMany().HasForeignKey(d => d.B_Id).WillCascadeOnDelete(false);
            modelBuilder.Entity<D>().HasRequired(d => d.C).WithMany().HasForeignKey(d => d.C_Id).WillCascadeOnDelete(false);

            modelBuilder.Entity<E>().HasRequired(e => e.B).WithMany().HasForeignKey(e => e.B_Id).WillCascadeOnDelete(false);

            modelBuilder.Entity<F>().HasRequired(f => f.D).WithMany().HasForeignKey(f => f.D_Id).WillCascadeOnDelete(false);
            modelBuilder.Entity<F>().HasRequired(f => f.E).WithMany().HasForeignKey(f => f.E_Id).WillCascadeOnDelete(false);

            modelBuilder.Entity<G>().HasRequired(g => g.D).WithMany().HasForeignKey(g => g.D_Id).WillCascadeOnDelete(false);

            modelBuilder.Entity<H>().HasRequired(h => h.E).WithMany().HasForeignKey(h => h.E_Id).WillCascadeOnDelete(false);
            modelBuilder.Entity<H>().HasRequired(h => h.C).WithMany().HasForeignKey(h => h.C_Id).WillCascadeOnDelete(false);
        }
    }
    abstract class Entity
    {
        public Guid Id { get; set; }

        protected Entity()
        {
            Id = Guid.NewGuid();
        }
    }
    class A : Entity
    {
        
    }
    class B : Entity
    {

    }
    class C : Entity
    {
        
    }
    class D : Entity
    {
        public Guid A_Id { get; set; }
        public A A { get; set; }

        public Guid B_Id { get; set; }
        public B B { get; set; }

        public Guid C_Id { get; set; }
        public C C { get; set; }
    }
    class E : Entity
    {
        public Guid B_Id { get; set; }
        public B B { get; set; }
    }
    class F : Entity
    {
        public Guid D_Id { get; set; }
        public D D { get; set; }

        public Guid E_Id { get; set; }
        public E E { get; set; }
    }
    class G : Entity
    {
        public Guid D_Id { get; set; }
        public D D { get; set; }
    }
    class H : Entity
    {
        public Guid E_Id { get; set; }
        public E E { get; set; }

        public Guid C_Id { get; set; }
        public C C { get; set; }
    }
}
