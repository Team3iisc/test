fc <- ts(numeric(length(test)), start=start1, deltat = 1)

for(wind in 1:num_retrain)
{
  x1 <- window(temp, end=end1 + wind_size*(wind-1))
  refit <- Arima(x1, model = fit)
  temp_forecast <- forecast(refit, h=10)$mean
  for(i in 1:wind_size)
  {
    fc[wind_size*(wind-1)+i] <- temp_forecast[i]
  }  
}