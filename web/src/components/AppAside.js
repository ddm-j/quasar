import React, { useState } from 'react'
import { useSelector, useDispatch } from 'react-redux'
import {
  CCloseButton,
  CFormSwitch,
  CNav,
  CNavItem,
  CNavLink,
  CTabContent,
  CTabPane,
  CProgress,
  CSidebar,
  CSidebarHeader,
} from '@coreui/react-pro'
import CIcon from '@coreui/icons-react'
import { cilSettings } from '@coreui/icons'

const AppAside = () => {
  const dispatch = useDispatch()
  const asideShow = useSelector((state) => state.asideShow)

  const [activeKey, setActiveKey] = useState(1)

  return (
    <CSidebar
      className="border-start"
      colorScheme="light"
      size="lg"
      overlaid
      placement="end"
      visible={asideShow}
      onVisibleChange={(visible) => {
        dispatch({ type: 'set', asideShow: visible })
      }}
    >
      <CSidebarHeader className="p-0 position-relative">
        <CNav className="w-100" variant="underline-border">
          <CNavItem>
            <CNavLink
              href="#"
              active={activeKey === 1}
              onClick={(e) => {
                e.preventDefault()
                setActiveKey(1)
              }}
            >
              <CIcon icon={cilSettings} />
            </CNavLink>
          </CNavItem>
        </CNav>
        <CCloseButton
          className="position-absolute top-50 end-0 translate-middle my-0"
          onClick={() => dispatch({ type: 'set', asideShow: false })}
        />
      </CSidebarHeader>
      <CTabContent>
        <CTabPane className="p-3" visible={activeKey === 1}>
          <h6>Settings</h6>
          <div>
            <div className="clearfix mt-4">
              <CFormSwitch size="lg" label="Option 1" id="Option1" defaultChecked />
            </div>
            <div>
              <small className="text-body-secondary">
                Lorem ipsum dolor sit amet, consectetur adipisicing elit.
              </small>
            </div>
          </div>
          <div>
            <div className="clearfix mt-3">
              <CFormSwitch size="lg" label="Option 2" id="Option2" />
            </div>
            <div>
              <small className="text-body-secondary">
                Lorem ipsum dolor sit amet, consectetur adipisicing elit.
              </small>
            </div>
          </div>
          <div>
            <div className="clearfix mt-3">
              <CFormSwitch size="lg" label="Option 3" id="Option3" />
            </div>
          </div>
          <div>
            <div className="clearfix mt-3">
              <CFormSwitch size="lg" label="Option 4" id="Option4" defaultChecked />
            </div>
          </div>
          <hr />
          <h6>System Utilization</h6>
          <div className="text-uppercase small fw-semibold mb-1 mt-4">CPU Usage</div>
          <CProgress thin color="info-gradient" value={25} />
          <div className="text-body-secondary small">348 Processes. 1/4 Cores.</div>
          <div className="text-uppercase small fw-semibold mb-1 mt-2">Memory Usage</div>
          <CProgress thin color="warning-gradient" value={70} />
          <div className="text-body-secondary small">11444GB/16384MB</div>
          <div className="text-uppercase small fw-semibold mb-1 mt-2">SSD 1 Usage</div>
          <CProgress thin color="danger-gradient" value={95} />
          <div className="text-body-secondary small">243GB/256GB</div>
          <div className="text-uppercase small fw-semibold mb-1 mt-2">SSD 2 Usage</div>
          <CProgress thin color="success-gradient" value={10} />
          <div className="text-body-secondary small">25GB/256GB</div>
        </CTabPane>
      </CTabContent>
    </CSidebar>
  )
}

export default React.memo(AppAside)
