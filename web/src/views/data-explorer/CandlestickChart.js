import React, { useEffect, useRef, useState } from 'react'
import PropTypes from 'prop-types'
import { createChart } from 'lightweight-charts'
import { getOHLCData } from '../services/datahub_api'
import CIcon from '@coreui/icons-react'
import {
  cilReload,
  cilFullscreen,
  cilChevronLeft,
  cilChevronRight,
} from '@coreui/icons'

const CandlestickChart = ({ provider, symbol, dataType, interval, limit = 5000 }) => {
  const chartContainerRef = useRef(null)
  const chartRef = useRef(null)
  const candlestickSeriesRef = useRef(null)
  const cancelledRef = useRef(false)
  const resizeTimeoutRef = useRef(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [chartData, setChartData] = useState([])
  const [initialViewRange, setInitialViewRange] = useState(null)
  const [chartDimensions, setChartDimensions] = useState({ width: 0, height: 500 })

  // Calculate chart height based on container width with responsive aspect ratios
  const calculateChartHeight = (containerWidth) => {
    let aspectRatio
    let minHeight
    let maxHeight
    
    if (containerWidth >= 992) {
      // Desktop (lg, xl)
      aspectRatio = 2.5
      minHeight = 500
      maxHeight = 800
    } else if (containerWidth >= 768) {
      // Tablet (md)
      aspectRatio = 2.25
      minHeight = 400
      maxHeight = 600
    } else {
      // Mobile (xs, sm)
      aspectRatio = 1.75
      minHeight = 300
      maxHeight = 400
    }
    
    const calculatedHeight = containerWidth / aspectRatio
    return Math.max(minHeight, Math.min(maxHeight, calculatedHeight))
  }

  // Update chart dimensions based on container size
  const updateChartDimensions = () => {
    if (!chartContainerRef.current) return { width: 0, height: 500 }
    
    const containerWidth = chartContainerRef.current.clientWidth
    const calculatedHeight = calculateChartHeight(containerWidth)
    
    const dimensions = {
      width: containerWidth,
      height: calculatedHeight,
    }
    
    setChartDimensions(dimensions)
    return dimensions
  }

  // Calculate initial dimensions when container is available
  useEffect(() => {
    if (chartContainerRef.current) {
      updateChartDimensions()
    }
  }, [])

  // Initialize chart (only once)
  useEffect(() => {
    if (!chartContainerRef.current) return

    let chart = null
    let candlestickSeries = null
    let handleResize = null

    // Calculate initial dimensions
    const initialDimensions = updateChartDimensions()
    if (initialDimensions.width === 0) return

    try {
      // Create chart instance
      chart = createChart(chartContainerRef.current, {
        layout: {
          background: { color: '#000000' },
          textColor: '#ffffff',
        },
        width: initialDimensions.width,
        height: initialDimensions.height,
        grid: {
          vertLines: { color: '#1a1a1a' },
          horzLines: { color: '#1a1a1a' },
        },
        crosshair: {
          mode: 1, // Normal crosshair
          vertLine: {
            color: '#758696',
            width: 1,
            style: 0,
            labelBackgroundColor: '#1e1e1e',
          },
          horzLine: {
            color: '#758696',
            width: 1,
            style: 0,
            labelBackgroundColor: '#1e1e1e',
          },
        },
        rightPriceScale: {
          borderColor: '#2a2e39',
          scaleMargins: {
            top: 0.1,
            bottom: 0.1,
          },
          entireRangeOnly: true,  // Prevent scrolling beyond data range (min/max)
        },
        timeScale: {
          borderColor: '#2a2e39',
          timeVisible: true,
          secondsVisible: false,
          fixLeftEdge: true,  // Prevent scrolling past the first (oldest) bar
          fixRightEdge: true, // Prevent scrolling past the last (newest) bar
        },
      })

      chartRef.current = chart

      // Add candlestick series
      candlestickSeries = chart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderVisible: false,
        wickUpColor: '#26a69a',
        wickDownColor: '#ef5350',
      })

      candlestickSeriesRef.current = candlestickSeries

      // Handle resize with debouncing
      handleResize = () => {
        // Clear existing timeout
        if (resizeTimeoutRef.current) {
          clearTimeout(resizeTimeoutRef.current)
        }
        
        // Debounce resize events (150ms)
        resizeTimeoutRef.current = setTimeout(() => {
          if (chartContainerRef.current && chart) {
            const dimensions = updateChartDimensions()
            chart.applyOptions({
              width: dimensions.width,
              height: dimensions.height,
            })
          }
        }, 150)
      }

      window.addEventListener('resize', handleResize)
    } catch (err) {
      console.error('Error initializing chart:', err)
      // Clean up partial initialization
      if (chart) {
        try {
          chart.remove()
        } catch (cleanupErr) {
          console.error('Error cleaning up chart during initialization:', cleanupErr)
        }
      }
    }

    // Cleanup function - always runs
    return () => {
      if (resizeTimeoutRef.current) {
        clearTimeout(resizeTimeoutRef.current)
      }
      if (handleResize) {
        window.removeEventListener('resize', handleResize)
      }
      if (chart) {
        try {
          chart.remove()
        } catch (err) {
          console.error('Error removing chart during cleanup:', err)
        }
      }
      // Clear refs
      chartRef.current = null
      candlestickSeriesRef.current = null
    }
  }, [])

  // Fetch data when props change
  useEffect(() => {
    if (!provider || !symbol || !dataType || !interval) {
      // Clear chart if required props are missing
      if (candlestickSeriesRef.current) {
        candlestickSeriesRef.current.setData([])
      }
      setChartData([])
      setInitialViewRange(null)
      setError(null)
      return
    }

    // Reset cancellation flag for new request
    cancelledRef.current = false

    const fetchData = async () => {
      setLoading(true)
      setError(null)

      try {
        const response = await getOHLCData(provider, symbol, dataType, interval, {
          limit: limit,
          order: 'desc', // Get most recent data first
        })

        // Check if cancelled before updating state
        if (cancelledRef.current) return

        if (response.bars && response.bars.length > 0) {
          // Transform API response to chart format
          // Backend returns: { time: int, open, high, low, close, volume }
          // Chart expects: { time: number, open, high, low, close }
          // Sort by time ascending (data comes desc from API, chart needs asc)
          const chartData = response.bars
            .map(bar => ({
              time: bar.time,
              open: bar.open,
              high: bar.high,
              low: bar.low,
              close: bar.close,
            }))
            .sort((a, b) => a.time - b.time) // Sort ascending for chart

          if (candlestickSeriesRef.current && !cancelledRef.current) {
            candlestickSeriesRef.current.setData(chartData)
            
            // Store chart data for button functions
            setChartData(chartData)
            
            // Set initial visible range to show only the most recent 500 bars (1/10th of total)
            const visibleBarCount = 500
            if (chartData.length > visibleBarCount && chartRef.current) {
              const timeScale = chartRef.current.timeScale()
              // Get the most recent 500 bars (last 500 in sorted array)
              const visibleStartIndex = chartData.length - visibleBarCount
              const visibleStartTime = chartData[visibleStartIndex].time
              const visibleEndTime = chartData[chartData.length - 1].time
              
              // Store initial view range for Reset View button
              const initialRange = {
                from: visibleStartTime,
                to: visibleEndTime,
              }
              setInitialViewRange(initialRange)
              
              // Set visible range to show only the most recent bars
              timeScale.setVisibleRange(initialRange)
            } else {
              // If less than 500 bars, store range for all data
              if (chartData.length > 0) {
                const allDataRange = {
                  from: chartData[0].time,
                  to: chartData[chartData.length - 1].time,
                }
                setInitialViewRange(allDataRange)
              }
            }
          }
        } else {
          if (!cancelledRef.current) {
            if (candlestickSeriesRef.current) {
              candlestickSeriesRef.current.setData([])
            }
            setChartData([])
            setInitialViewRange(null)
            setError('No data available for the selected symbol and interval.')
          }
        }
      } catch (err) {
        // Only update state if not cancelled
        if (!cancelledRef.current) {
          console.error('Error fetching OHLC data:', err)
          setError(err.message || 'Failed to load chart data')
          if (candlestickSeriesRef.current) {
            candlestickSeriesRef.current.setData([])
          }
        }
      } finally {
        if (!cancelledRef.current) {
          setLoading(false)
        }
      }
    }

    fetchData()

    // Cleanup function to cancel request if props change
    return () => {
      cancelledRef.current = true
    }
  }, [provider, symbol, dataType, interval, limit])

  // Button handler functions
  const handleResetView = () => {
    if (!chartRef.current || chartData.length === 0) return

    const timeScale = chartRef.current.timeScale()

    // If we have initial range, use it; otherwise show all
    if (initialViewRange) {
      timeScale.setVisibleRange(initialViewRange)
    } else {
      // Fallback: show all data
      if (chartData.length > 0) {
        timeScale.setVisibleRange({
          from: chartData[0].time,
          to: chartData[chartData.length - 1].time,
        })
      }
    }
  }

  const handleShowAll = () => {
    if (!chartRef.current || chartData.length === 0) return

    const timeScale = chartRef.current.timeScale()
    timeScale.setVisibleRange({
      from: chartData[0].time,
      to: chartData[chartData.length - 1].time,
    })
  }

  const handleJumpToStart = () => {
    if (!chartRef.current || chartData.length === 0) return

    const timeScale = chartRef.current.timeScale()
    const visibleBarCount = 500
    const firstBarTime = chartData[0].time

    // Show first 500 bars (or all if less than 500)
    const endIndex = Math.min(visibleBarCount - 1, chartData.length - 1)
    const endTime = chartData[endIndex].time

    timeScale.setVisibleRange({
      from: firstBarTime,
      to: endTime,
    })
  }

  const handleJumpToLatest = () => {
    if (!chartRef.current || chartData.length === 0) return

    const timeScale = chartRef.current.timeScale()
    const visibleBarCount = 500

    if (chartData.length > visibleBarCount) {
      const visibleStartIndex = chartData.length - visibleBarCount
      const visibleStartTime = chartData[visibleStartIndex].time
      const visibleEndTime = chartData[chartData.length - 1].time

      timeScale.setVisibleRange({
        from: visibleStartTime,
        to: visibleEndTime,
      })
    } else {
      // Show all if less than 500 bars
      timeScale.setVisibleRange({
        from: chartData[0].time,
        to: chartData[chartData.length - 1].time,
      })
    }
  }

  return (
    <div style={{ position: 'relative', width: '100%', height: `${chartDimensions.height}px` }}>
      {loading && (
        <div
          style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            color: '#ffffff',
            zIndex: 10,
            textAlign: 'center',
          }}
        >
          <div>Loading chart data...</div>
        </div>
      )}
      {error && !loading && (
        <div
          style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            color: '#ef5350',
            zIndex: 10,
            textAlign: 'center',
            padding: '20px',
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            borderRadius: '4px',
          }}
        >
          <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>Error</div>
          <div>{error}</div>
        </div>
      )}
      {!provider || !symbol || !dataType || !interval ? (
        <div
          style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            color: '#ffffff',
            zIndex: 10,
            textAlign: 'center',
            padding: '20px',
            backgroundColor: 'rgba(0, 0, 0, 0.8)',
            borderRadius: '4px',
          }}
        >
          <div>Please select a symbol, data type, and interval to view the chart.</div>
        </div>
      ) : null}
      {/* Chart Control Buttons */}
      {chartData.length > 0 && !loading && (
        <div
          style={{
            position: 'absolute',
            top: '10px',
            left: '10px',
            zIndex: 10,
            display: 'flex',
            gap: '6px',
            flexWrap: 'wrap',
          }}
        >
          <button
            onClick={handleResetView}
            style={{
              padding: '8px',
              backgroundColor: 'rgba(42, 46, 57, 0.9)',
              color: '#ffffff',
              border: '1px solid #2a2e39',
              borderRadius: '4px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              minWidth: '36px',
              minHeight: '36px',
              transition: 'background-color 0.2s',
            }}
            onMouseEnter={(e) => {
              e.target.style.backgroundColor = 'rgba(58, 64, 77, 0.9)'
            }}
            onMouseLeave={(e) => {
              e.target.style.backgroundColor = 'rgba(42, 46, 57, 0.9)'
            }}
            title="Reset to default view (500 most recent bars)"
          >
            <CIcon icon={cilReload} size="sm" />
          </button>
          <button
            onClick={handleShowAll}
            style={{
              padding: '8px',
              backgroundColor: 'rgba(42, 46, 57, 0.9)',
              color: '#ffffff',
              border: '1px solid #2a2e39',
              borderRadius: '4px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              minWidth: '36px',
              minHeight: '36px',
              transition: 'background-color 0.2s',
            }}
            onMouseEnter={(e) => {
              e.target.style.backgroundColor = 'rgba(58, 64, 77, 0.9)'
            }}
            onMouseLeave={(e) => {
              e.target.style.backgroundColor = 'rgba(42, 46, 57, 0.9)'
            }}
            title="Show all 5000 bars"
          >
            <CIcon icon={cilFullscreen} size="sm" />
          </button>
          <button
            onClick={handleJumpToStart}
            style={{
              padding: '8px',
              backgroundColor: 'rgba(42, 46, 57, 0.9)',
              color: '#ffffff',
              border: '1px solid #2a2e39',
              borderRadius: '4px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              minWidth: '36px',
              minHeight: '36px',
              transition: 'background-color 0.2s',
            }}
            onMouseEnter={(e) => {
              e.target.style.backgroundColor = 'rgba(58, 64, 77, 0.9)'
            }}
            onMouseLeave={(e) => {
              e.target.style.backgroundColor = 'rgba(42, 46, 57, 0.9)'
            }}
            title="Jump to oldest data (first 500 bars)"
          >
            <CIcon icon={cilChevronLeft} size="sm" />
          </button>
          <button
            onClick={handleJumpToLatest}
            style={{
              padding: '8px',
              backgroundColor: 'rgba(42, 46, 57, 0.9)',
              color: '#ffffff',
              border: '1px solid #2a2e39',
              borderRadius: '4px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              minWidth: '36px',
              minHeight: '36px',
              transition: 'background-color 0.2s',
            }}
            onMouseEnter={(e) => {
              e.target.style.backgroundColor = 'rgba(58, 64, 77, 0.9)'
            }}
            onMouseLeave={(e) => {
              e.target.style.backgroundColor = 'rgba(42, 46, 57, 0.9)'
            }}
            title="Jump to newest data (last 500 bars)"
          >
            <CIcon icon={cilChevronRight} size="sm" />
          </button>
        </div>
      )}
      <div
        ref={chartContainerRef}
        style={{
          width: '100%',
          height: `${chartDimensions.height}px`,
          backgroundColor: '#000000',
          borderRadius: '4px',
          opacity: loading ? 0.5 : 1,
        }}
      />
    </div>
  )
}

CandlestickChart.propTypes = {
  provider: PropTypes.string,
  symbol: PropTypes.string,
  dataType: PropTypes.oneOf(['historical', 'live']),
  interval: PropTypes.string,
  limit: PropTypes.number,
}

CandlestickChart.defaultProps = {
  provider: null,
  symbol: null,
  dataType: null,
  interval: null,
  limit: 5000,
}

export default CandlestickChart

