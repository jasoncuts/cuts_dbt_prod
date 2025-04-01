WITH line_discount AS 
(
    SELECT order_line_id
        ,order_id
        ,
  
    sum(
      
      case
      when d.index = 0
        then amount
      else 0
      end
    )
    
      
            as `dis_0_amt`
      
    
    ,
  
    sum(
      
      case
      when d.index = 1
        then amount
      else 0
      end
    )
    
      
            as dis_1_amt
      
    
    ,
  
    sum(
      
      case
      when d.index = 2
        then amount
      else 0
      end
    )
    
      
            as `dis_2_amt`
      
    
    ,
  
    sum(
      
      case
      when d.index = 3
        then amount
      else 0
      end
    )
    
      
            as `dis_3_amt`
      
    
    
  

        ,
  
    max(
      
      case
      when d.index = 0 
        then discount_application_index
      else 0
      end
    )
    
      
            as `dis_0_idx`
      
    
    ,
  
    max(
      
      case
      when d.index = 1
        then discount_application_index
      else 0
      end
    )
    
      
            as `dis_1_idx`
      
    
    ,
  
    max(
      
      case
      when d.index = 2
        then discount_application_index
      else 0
      end
    )
    
      
            as `dis_2_idx`
      
    
    ,
  
    max(
      
      case
      when d.index = 3
        then discount_application_index
      else 0
      end
    )
    
      
            as `dis_3_idx`
      
    
    
  

    FROM {{source('shopify_us','lineitems_discount')}} d 
    LEFT JOIN {{source('shopify_us', 'lineitems')}} l 
    ON d.order_line_id = l.id 
    GROUP BY 1,2 
)

SELECT *
FROM line_discount
