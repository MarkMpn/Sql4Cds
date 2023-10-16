using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace MarkMpn.Sql4Cds.Engine
{
    public static class TSqlFragmentExtensions
    {
        private static ConcurrentDictionary<Type, Func<TSqlFragment, TSqlFragment>> _fragmentCloneMethods = new ConcurrentDictionary<Type, Func<TSqlFragment, TSqlFragment>>();

        /// <summary>
        /// Converts a <see cref="TSqlFragment"/> to the corresponding SQL string
        /// </summary>
        /// <param name="fragment">The SQL DOM fragment to convert</param>
        /// <returns>The SQL string that the fragment can be parsed from</returns>
        public static string ToSql(this TSqlFragment fragment)
        {
            if (fragment.ScriptTokenStream != null)
            {
                return String.Join("",
                    fragment.ScriptTokenStream
                        .Skip(fragment.FirstTokenIndex)
                        .Take(fragment.LastTokenIndex - fragment.FirstTokenIndex + 1)
                        .Select(t => t.Text));
            }

            new Sql160ScriptGenerator().GenerateScript(fragment, out var sql);
            return sql;
        }

        /// <summary>
        /// Creates a clone of a <see cref="TSqlFragment"/>
        /// </summary>
        /// <typeparam name="T">The type of <see cref="TSqlFragment"/> being cloned</typeparam>
        /// <param name="fragment">The fragment to clone</param>
        /// <returns>A clone of the requested <paramref name="fragment"/></returns>
        public static T Clone<T>(this T fragment) where T : TSqlFragment
        {
            if (fragment == null)
                return null;

            var cloner = _fragmentCloneMethods.GetOrAdd(fragment.GetType(), CreateCloneMethod);

            return (T)cloner(fragment);
        }

        private static Func<TSqlFragment, TSqlFragment> CreateCloneMethod(Type type)
        {
            var variables = new List<ParameterExpression>();
            var paramExpr = Expression.Parameter(typeof(TSqlFragment));
            var typedParam = Expression.Variable(type);
            variables.Add(typedParam);
            var clone = Expression.Variable(type);
            variables.Add(clone);
            var loopIndex = Expression.Variable(typeof(int));
            variables.Add(loopIndex);
            var body = new List<Expression>();

            body.Add(Expression.Assign(typedParam, Expression.Convert(paramExpr, type)));
            body.Add(Expression.Assign(clone, Expression.New(type.GetConstructor(Array.Empty<Type>()))));

            // Loop over all properties and generate get/clone/set expressions
            // For IList<T> properties, loop over all values and clone/add
            foreach (var prop in type.GetProperties())
            {
                if (prop.GetIndexParameters().Length > 0)
                    continue;

                if (prop.Name != nameof(TSqlFragment.ScriptTokenStream) && prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(IList<>))
                {
                    var itemType = prop.PropertyType.GetGenericArguments()[0];
                    var collectionType = typeof(ICollection<>).MakeGenericType(itemType);
                    var indexerProp = prop.PropertyType.GetProperties().Single(p => p.GetIndexParameters().Length == 1);

                    var loopEnd = Expression.Label();
                    body.Add(Expression.Assign(loopIndex, Expression.Constant(0)));
                    body.Add(
                        Expression.Loop(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.Equal(
                                        loopIndex,
                                        Expression.Property(
                                            Expression.Convert(
                                                Expression.Property(typedParam, prop),
                                                collectionType
                                                ),
                                            nameof(ICollection<object>.Count)
                                            )
                                        ),
                                    Expression.Break(loopEnd)
                                    ),
                                Expression.Call(
                                    Expression.Convert(Expression.Property(clone, prop), collectionType),
                                    nameof(ICollection<object>.Add),
                                    Array.Empty<Type>(),
                                    typeof(TSqlFragment).IsAssignableFrom(itemType)
                                    ? (Expression)Expression.Call(typeof(TSqlFragmentExtensions).GetMethod(nameof(Clone)).MakeGenericMethod(itemType), Expression.MakeIndex(Expression.Property(typedParam, prop), indexerProp, new[] { loopIndex }))
                                    : Expression.MakeIndex(Expression.Property(typedParam, prop), indexerProp, new[] { loopIndex })
                                    ),
                                Expression.PostIncrementAssign(loopIndex)
                                ),
                            loopEnd
                            )
                        );
                }
                else if (prop.CanWrite)
                {
                    if (typeof(TSqlFragment).IsAssignableFrom(prop.PropertyType))
                    {
                        body.Add(
                            Expression.Assign(
                                Expression.Property(clone, prop),
                                Expression.Call(typeof(TSqlFragmentExtensions).GetMethod(nameof(Clone)).MakeGenericMethod(prop.PropertyType), Expression.Property(typedParam, prop))
                                )
                            );
                    }
                    else
                    {
                        body.Add(
                            Expression.Assign(
                                Expression.Property(clone, prop),
                                Expression.Property(typedParam, prop)
                                )
                            );
                    }
                }
            }

            var returnTarget = Expression.Label(typeof(TSqlFragment));
            body.Add(Expression.Return(returnTarget, clone));
            body.Add(Expression.Label(returnTarget, clone));

            var block = Expression.Block(typeof(TSqlFragment), variables, body);
            return Expression.Lambda<Func<TSqlFragment,TSqlFragment>>(block, paramExpr).Compile();
        }
    }
}
