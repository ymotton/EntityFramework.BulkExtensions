using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Linq;

namespace EntityFramework.BulkExtensions
{
    public static class DbContextBulkExtensions
    {
        public static int BulkSaveAdditions(this DbContext context)
        {
            GuardAgainstOtherChanges(context);

            var addedEntities = ((IObjectContextAdapter) context)
                .ObjectContext
                .ObjectStateManager
                .GetObjectStateEntries(EntityState.Added)
                .Where(e => e.EntityKey != null)
                .ToList();

            var entitiesPerType = addedEntities
                .GroupBy(EntityKey.Create, e => e.Entity)
                .ToList();

            var entitiesInTopologicalOrder = OrderTopologically(entitiesPerType);

            int count = 0;
            foreach (var entities in entitiesInTopologicalOrder)
            {
                context.BulkInsert(entities);
                count += entities.Count;
            }

            return count;
        }

        static void GuardAgainstOtherChanges(DbContext context)
        {
            var modifiedOrDeletedEntities = ((IObjectContextAdapter) context)
                .ObjectContext
                .ObjectStateManager
                .GetObjectStateEntries(EntityState.Deleted | EntityState.Modified)
                .Where(e => e.EntityKey != null)
                .ToList();

            if (modifiedOrDeletedEntities.Any())
            {
                var entitiesPerType = modifiedOrDeletedEntities
                    .GroupBy(EntityKey.Create, e => e.Entity)
                    .ToList();

                string entityTypesAndCount = string.Join(", ", entitiesPerType.Select(e =>
                    string.Format("{0}:{1}", e.Key.Name, e.Count())));

                throw new InvalidOperationException(
                    string.Format("BulkSaveAdditions only supports additions. "
                    + "Since we cannot ensure correctness of execution in combination with other alterations, please separate these actions into separate working sets. "
                    + "Following entity sets contain modifications ({0})", entityTypesAndCount));
            }
        }
        
        static void BulkInsert(this DbContext context, IEnumerable<object> entities)
        {
            var entitiesAsList = entities.ToList();
            var entityType = entitiesAsList.First().GetType();
            var provider = new NonGenericBulkInsertProvider();
            provider.BulkInsert(context, entityType, entitiesAsList);
        }

        /// <summary>
        /// http://en.wikipedia.org/wiki/Topological_sorting
        /// </summary>
        /// <param name="entitiesPerType"></param>
        /// <returns></returns>
        static IEnumerable<IReadOnlyCollection<object>> OrderTopologically(List<IGrouping<EntityKey, object>> entitiesPerType)
        {
            var sortedNodes = new List<EntityKey>();
            var allNodes = entitiesPerType.Select(g => g.Key).ToList();
            var nodesWithIncomingEdges = allNodes.Where(n => n.HasIncomingEdges).ToList();
            var nodesWithoutIncomingEdges = new Stack<EntityKey>(allNodes.Except(nodesWithIncomingEdges));

            while (nodesWithoutIncomingEdges.Any())
            {
                var nodeN = nodesWithoutIncomingEdges.Pop();
                sortedNodes.Add(nodeN);

                var nodesWithIncomingEdgesFromNodeN = nodesWithIncomingEdges.Where(n => n.HasIncomingEdgeFromNode(nodeN)).ToList();
                foreach (var nodeM in nodesWithIncomingEdgesFromNodeN)
                {
                    var rebasedNodeM = nodeM.ForgetEdgeFromNode(nodeN);
                    if (!rebasedNodeM.HasIncomingEdges)
                    {
                        nodesWithIncomingEdges.Remove(nodeM);
                        nodesWithoutIncomingEdges.Push(rebasedNodeM);
                    }
                    else
                    {
                        nodesWithIncomingEdges.Remove(nodeM);
                        nodesWithIncomingEdges.Add(rebasedNodeM);
                    }
                }
            }

            if (nodesWithIncomingEdges.Any())
            {
                Console.WriteLine("Nodes that cannot be ordered: {{{0}}}",
                    string.Join(", ", nodesWithIncomingEdges.Select(n => n.Name)));
            }

            var entitiesPerTypeLookup = entitiesPerType.ToDictionary(g => g.Key, g => g);
            return sortedNodes.Select(n => entitiesPerTypeLookup[n].ToList()).Reverse().ToList();
        }

        struct EntityKey
        {
            private readonly string _setName;
            private readonly List<string> _inboundEdges;

            public string Name { get { return _setName; } }
            public bool HasIncomingEdges { get { return _inboundEdges.Any(); } }
            public bool HasIncomingEdgeFromNode(EntityKey node)
            {
                return _inboundEdges.Contains(node._setName);
            }

            public EntityKey ForgetEdgeFromNode(EntityKey fromNode)
            {
                return new EntityKey(_setName, _inboundEdges.Where(e => e != fromNode._setName));
            }

            private EntityKey(string setName, IEnumerable<string> inboundEdges)
            {
                if (setName == null)
                {
                    throw new ArgumentNullException("setName");
                }
                _setName = setName;

                if (inboundEdges == null)
                {
                    throw new ArgumentNullException("inboundEdges");
                } 
                _inboundEdges = inboundEdges.ToList();
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return ((EntityKey)obj)._setName.Equals(_setName);
            }
            public override int GetHashCode()
            {
                return _setName.GetHashCode();
            }
            public override string ToString()
            {
                return string.Format(
                    "{0} - In:{{{1}}}",
                    _setName,
                    string.Join(", ", _inboundEdges));
            }

            public static EntityKey Create(ObjectStateEntry entry)
            {
                string entityName = entry.Entity.GetType().Name;

                var relationships = entry.RelationshipManager
                    .GetAllRelatedEnds()
                    .Select(r => r.RelationshipSet.ElementType)
                    .ToList();

                List<string> inboundEdges = new List<string>();
                foreach (var relationship in relationships)
                {
                    var relationshipEnds = relationship.Name.Split('_');

                    string leftTypeName = relationshipEnds[0];
                    string rightTypeName = relationshipEnds[1];
                    RelationshipMultiplicity leftMultiplicity = relationship.RelationshipEndMembers[0].RelationshipMultiplicity;
                    RelationshipMultiplicity rightMultiplicity = relationship.RelationshipEndMembers[1].RelationshipMultiplicity;

                    string theirTypeName;
                    RelationshipMultiplicity ourMultiplicity;
                    RelationshipMultiplicity theirMultiplicity;
                    if (entityName == rightTypeName)
                    {
                        theirTypeName = leftTypeName;
                        ourMultiplicity = rightMultiplicity;
                        theirMultiplicity = leftMultiplicity;
                    }
                    else
                    {
                        theirTypeName = rightTypeName;
                        ourMultiplicity = leftMultiplicity;
                        theirMultiplicity = rightMultiplicity;
                    }

                    bool isManyToMany = ourMultiplicity == RelationshipMultiplicity.Many && theirMultiplicity == RelationshipMultiplicity.Many;
                    bool isOptionalToOptional = ourMultiplicity == RelationshipMultiplicity.ZeroOrOne && theirMultiplicity == RelationshipMultiplicity.ZeroOrOne;
                    bool isSkippable = isManyToMany || isOptionalToOptional;
                    if (!isSkippable)
                    {
                        bool dependantOnUs = (ourMultiplicity == RelationshipMultiplicity.One || ourMultiplicity == RelationshipMultiplicity.ZeroOrOne) && (theirMultiplicity == RelationshipMultiplicity.Many)
                            || (ourMultiplicity == RelationshipMultiplicity.One && theirMultiplicity == RelationshipMultiplicity.ZeroOrOne);

                        if (dependantOnUs)
                        {
                            inboundEdges.Add(theirTypeName);
                        }
                    }
                }

                var entityKey = new EntityKey(entityName, inboundEdges);
                return entityKey;
            }
        }
    }
}
