package com.spark.sql.udf
import com.spark.sql.app.View
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, Literal}
import org.apache.spark.sql.types.{DataType, IntegerType}

object CountWindowFunction{

    case class CountWindowUDF(number:Expression) extends AggregateWindowFunction {
      self: Product =>

      override def children: Seq[Expression] = Seq(number)

      override def dataType: DataType = IntegerType

      protected val count = AttributeReference("currentCount", IntegerType, true)()

      override def aggBufferAttributes: Seq[AttributeReference] = Seq[AttributeReference](count)

      var viewObject: View = new View()

      override val initialValues: Seq[Expression] = {
        viewObject = new View()
        Seq(Literal(0))
      }
      override val updateExpressions: Seq[Expression] = {
        viewObject.addToNumber(0) //here I want to pass value of 'number' column
        //convert viewObject.getNumber() into Expression and return
        Seq(count)
      }
      override val evaluateExpression: Expression = aggBufferAttributes(0)

    }

  def getCurrentCount(num:Column):Column = withExpr( CountWindowUDF(num.expr) )
  private def withExpr(expr: Expression): Column = new Column(expr)

}
